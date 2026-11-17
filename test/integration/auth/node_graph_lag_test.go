/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	authorizationmetrics "k8s.io/apiserver/pkg/authorization/metrics"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/flowcontrol"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/ktesting"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/auth/nodeidentifier"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	volumecache "k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/populator"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	nodeauthorizer "k8s.io/kubernetes/plugin/pkg/auth/authorizer/node"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac/bootstrappolicy"
	"k8s.io/kubernetes/test/integration/framework"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

type nodeGraphPodGate struct {
	unassigned     chan struct{}
	blocked        chan struct{}
	released       chan struct{}
	unassignedOnce sync.Once
	blockedOnce    sync.Once
	releaseOnce    sync.Once
	uid            types.UID
}

func (g *nodeGraphPodGate) release() {
	g.releaseOnce.Do(func() { close(g.released) })
}

type nodeGraphGates struct {
	ctx   context.Context
	mu    sync.Mutex
	gates map[types.NamespacedName]*nodeGraphPodGate
}

func (g *nodeGraphGates) add(t *testing.T, namespace, name string, hold bool) *nodeGraphPodGate {
	t.Helper()
	key := types.NamespacedName{Namespace: namespace, Name: name}
	gate := &nodeGraphPodGate{
		unassigned: make(chan struct{}), blocked: make(chan struct{}), released: make(chan struct{}),
	}
	if !hold {
		gate.release()
	}
	g.mu.Lock()
	g.gates[key] = gate
	g.mu.Unlock()
	t.Cleanup(func() {
		gate.release()
		g.mu.Lock()
		delete(g.gates, key)
		g.mu.Unlock()
	})
	return gate
}

func (g *nodeGraphGates) deliver(obj interface{}, deliver func()) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		deliver()
		return
	}
	g.mu.Lock()
	gate := g.gates[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}]
	g.mu.Unlock()
	if gate == nil {
		deliver()
		return
	}
	if pod.Spec.NodeName == "" {
		deliver()
		gate.unassignedOnce.Do(func() {
			gate.uid = pod.UID
			close(gate.unassigned)
		})
		return
	}
	// Only the authorizer's notification is held. The informer store, binding
	// response, and independently served Pod watch remain free to advance.
	if pod.UID == gate.uid {
		gate.blockedOnce.Do(func() { close(gate.blocked) })
		select {
		case <-gate.released:
		case <-g.ctx.Done():
			return
		}
	}
	deliver()
}

type gatedNodePodHandler struct {
	cache.ResourceEventHandler
	gates *nodeGraphGates
}

func (h gatedNodePodHandler) OnAdd(obj interface{}, initial bool) {
	h.gates.deliver(obj, func() { h.ResourceEventHandler.OnAdd(obj, initial) })
}

func (h gatedNodePodHandler) OnUpdate(oldObj, newObj interface{}) {
	h.gates.deliver(newObj, func() { h.ResourceEventHandler.OnUpdate(oldObj, newObj) })
}

type gatedNodePodInformer struct {
	cache.SharedIndexInformer
	gates *nodeGraphGates
}

func (i gatedNodePodInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return i.SharedIndexInformer.AddEventHandler(gatedNodePodHandler{handler, i.gates})
}

type gatedNodePods struct {
	coreinformers.PodInformer
	informer cache.SharedIndexInformer
}

func (p gatedNodePods) Informer() cache.SharedIndexInformer { return p.informer }

type nodeGraphReplica struct {
	client clientset.Interface
	config *rest.Config
	gates  *nodeGraphGates
	pods   coreinformers.PodInformer
}

func startNodeGraphReplica(t *testing.T, ctx context.Context, storage *storagebackend.Config) *nodeGraphReplica {
	t.Helper()
	informerCtx, stopInformers := context.WithCancel(ctx)
	gates := &nodeGraphGates{ctx: informerCtx, gates: make(map[types.NamespacedName]*nodeGraphPodGate)}
	var factory informers.SharedInformerFactory
	client, config, stopServer := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.Etcd.StorageConfig = *storage
			opts.GenericServerRunOptions.AdvertiseAddress = net.ParseIP("127.0.0.1")
			opts.EndpointReconcilerType = "none"
			opts.Authorization.Modes = []string{"RBAC"}
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount", "TaintNodesByCondition"}
			opts.Admission.GenericAdmission.EnablePlugins = []string{"NodeRestriction"}
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			loopback, err := clientset.NewForConfig(config.ControlPlane.Generic.LoopbackClientConfig)
			require.NoError(t, err)
			factory = informers.NewSharedInformerFactory(loopback, 0)
			pods := factory.Core().V1().Pods()
			gatedPods := gatedNodePods{pods, gatedNodePodInformer{pods.Informer(), gates}}
			graph := nodeauthorizer.NewGraph()
			nodeauthorizer.AddGraphEventHandlers(informerCtx, graph,
				factory.Core().V1().Nodes(), gatedPods, factory.Core().V1().PersistentVolumes(),
				factory.Storage().V1().VolumeAttachments(), factory.Resource().V1().ResourceSlices(), nil)
			identifier := nodeidentifier.NewDefaultNodeIdentifier()
			nodeAuth := authorizationmetrics.InstrumentedAuthorizer("Node", "node-graph-lag",
				nodeauthorizer.NewAuthorizer(graph, identifier, bootstrappolicy.NodeRules()))
			delegate := config.ControlPlane.Generic.Authorization.Authorizer
			config.ControlPlane.Generic.Authorization.Authorizer = authorizer.AuthorizerFunc(func(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
				if _, isNode := identifier.NodeIdentity(attrs.GetUser()); isNode {
					return nodeAuth.Authorize(ctx, attrs)
				}
				return delegate.Authorize(ctx, attrs)
			})
			config.ControlPlane.Generic.AddPostStartHookOrDie("node-graph-lag-informers", func(genericapiserver.PostStartHookContext) error {
				factory.Start(informerCtx.Done())
				return nil
			})
		},
	})
	t.Cleanup(func() {
		stopInformers()
		factory.Shutdown()
		stopServer()
	})
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for informer, synced := range factory.WaitForCacheSync(syncCtx.Done()) {
		require.True(t, synced, "informer %v did not synchronize", informer)
	}
	return &nodeGraphReplica{client: client, config: config, gates: gates, pods: factory.Core().V1().Pods()}
}

func (r *nodeGraphReplica) nodeClient(t *testing.T, name string) clientset.Interface {
	t.Helper()
	client, err := clientset.NewForConfig(r.nodeConfig(name))
	require.NoError(t, err)
	return client
}

func (r *nodeGraphReplica) nodeConfig(name string) *rest.Config {
	config := rest.CopyConfig(r.config)
	config.Impersonate = rest.ImpersonationConfig{UserName: "system:node:" + name, Groups: []string{"system:nodes"}}
	return config
}

type nodeGraphVolumeRead struct {
	resource string
	status   int
}

type nodeGraphVolumeEndpoint struct {
	url       *url.URL
	transport http.RoundTripper
}

type nodeGraphVolumeTransport struct {
	pvc, pv nodeGraphVolumeEndpoint
	mu      sync.Mutex
	reads   []nodeGraphVolumeRead
	onRead  func(nodeGraphVolumeRead)
}

func (r *nodeGraphVolumeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var endpoint nodeGraphVolumeEndpoint
	var resource string
	switch {
	case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/persistentvolumeclaims/"):
		endpoint, resource = r.pvc, "PVC"
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/api/v1/persistentvolumes/"):
		endpoint, resource = r.pv, "PV"
	default:
		return nil, fmt.Errorf("unexpected volume lookup: %s %s", request.Method, request.URL.Path)
	}
	routed := request.Clone(request.Context())
	routed.URL.Scheme = endpoint.url.Scheme
	routed.URL.Host = endpoint.url.Host
	routed.Host = endpoint.url.Host
	response, err := endpoint.transport.RoundTrip(routed)
	if err != nil {
		return nil, err
	}
	read := nodeGraphVolumeRead{resource, response.StatusCode}
	r.mu.Lock()
	r.reads = append(r.reads, read)
	r.mu.Unlock()
	if r.onRead != nil {
		r.onRead(read)
	}
	return response, nil
}

func (r *nodeGraphVolumeTransport) snapshot() []nodeGraphVolumeRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]nodeGraphVolumeRead(nil), r.reads...)
}

func nodeGraphVolumeClient(t *testing.T, pvcReplica, pvReplica *nodeGraphReplica, limiter flowcontrol.RateLimiter) (clientset.Interface, *nodeGraphVolumeTransport) {
	t.Helper()
	endpoint := func(replica *nodeGraphReplica) nodeGraphVolumeEndpoint {
		cfg := replica.nodeConfig("node1")
		target, err := url.Parse(cfg.Host)
		require.NoError(t, err)
		transport, err := rest.TransportFor(cfg)
		require.NoError(t, err)
		return nodeGraphVolumeEndpoint{target, transport}
	}
	transport := &nodeGraphVolumeTransport{pvc: endpoint(pvcReplica), pv: endpoint(pvReplica)}
	client, err := clientset.NewForConfig(&rest.Config{
		Host: pvcReplica.config.Host, Transport: transport, QPS: 100, Burst: 100, RateLimiter: limiter,
	})
	require.NoError(t, err)
	return client, transport
}

type nodeGraphSteppedPods struct {
	ctx     context.Context
	pods    []*v1.Pod // Updated only while the populator is held at a pass barrier.
	entered chan struct{}
	proceed chan struct{}
}

func (p *nodeGraphSteppedPods) GetPods() []*v1.Pod {
	select {
	case p.entered <- struct{}{}:
	case <-p.ctx.Done():
		return nil
	}
	select {
	case <-p.proceed:
		return p.pods
	case <-p.ctx.Done():
		return nil
	}
}

func (p *nodeGraphSteppedPods) GetPodByUID(uid types.UID) (*v1.Pod, bool) {
	for _, pod := range p.pods {
		if uid == pod.UID {
			return pod, true
		}
	}
	return nil, false
}

type nodeGraphActivePod struct{}

func (nodeGraphActivePod) ShouldPodContainersBeTerminating(types.UID) bool { return false }
func (nodeGraphActivePod) ShouldPodRuntimeBeRemoved(types.UID) bool        { return false }

type nodeGraphVolumeErrors struct {
	volumecache.DesiredStateOfWorld
	mu        sync.Mutex
	published []nodeGraphPublishedError
}

type nodeGraphPublishedError struct {
	message      string
	backoffStart time.Time
}

func (d *nodeGraphVolumeErrors) AddErrorToPod(podName volumetypes.UniquePodName, err string) {
	d.DesiredStateOfWorld.AddErrorToPod(podName, err)
	d.mu.Lock()
	d.published = append(d.published, nodeGraphPublishedError{message: err})
	d.mu.Unlock()
}

func (d *nodeGraphVolumeErrors) AddErrorToPodWithBackoffStart(podName volumetypes.UniquePodName, err string, started time.Time) {
	d.DesiredStateOfWorld.AddErrorToPodWithBackoffStart(podName, err, started)
	d.mu.Lock()
	d.published = append(d.published, nodeGraphPublishedError{message: err, backoffStart: started})
	d.mu.Unlock()
}

func (d *nodeGraphVolumeErrors) snapshot() []nodeGraphPublishedError {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]nodeGraphPublishedError(nil), d.published...)
}

type nodeGraphVolumeRetryOptions struct {
	blockedRetries    int
	exhaust           bool
	freshPod          *v1.Pod
	denyRetryCapacity bool
}

type nodeGraphVolumeLimiter struct {
	flowcontrol.RateLimiter // Replaced only while the populator is held at a pass barrier.
}

func testNodeGraphVolumeRetry(t *testing.T, parent context.Context, retryPods []*v1.Pod, pv *v1.PersistentVolume, pvcReplica, pvReplica *nodeGraphReplica, gate *nodeGraphPodGate, options nodeGraphVolumeRetryOptions) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	blockedRetries, exhaust, freshPod := options.blockedRetries, options.exhaust, options.freshPod
	limiter := &nodeGraphVolumeLimiter{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(100, 100)}
	if options.denyRetryCapacity {
		tokens := len(retryPods)
		if pvcReplica != pvReplica {
			tokens *= 2
		}
		limiter.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(0, tokens)
	}
	client, transport := nodeGraphVolumeClient(t, pvcReplica, pvReplica, limiter)
	pluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	dsw := &nodeGraphVolumeErrors{DesiredStateOfWorld: volumecache.NewDesiredStateOfWorld(pluginMgr, volumeutil.NewFakeSELinuxLabelTranslator())}
	asw := volumecache.NewActualStateOfWorld(types.NodeName(retryPods[0].Spec.NodeName), pluginMgr, nil)
	pods := &nodeGraphSteppedPods{
		ctx: ctx, pods: retryPods, entered: make(chan struct{}), proceed: make(chan struct{}),
	}
	clock := clocktesting.NewFakeClock(time.Unix(1, 0))
	translator := csitrans.New()
	pop := populator.NewDesiredStateOfWorldPopulator(
		client, 100*time.Millisecond, pods, nodeGraphActivePod{}, dsw, asw,
		csimigration.NewPluginManager(translator), translator, pluginMgr, clock)
	retryNames := sets.New[volumetypes.UniquePodName]()
	reprocess := func() {
		for _, pod := range retryPods {
			podName := volumeutil.GetUniquePodName(pod)
			retryNames.Insert(podName)
			pop.ReprocessPod(podName)
		}
	}
	reprocess()
	addFresh := func() {
		pods.pods = append(pods.pods, freshPod)
		pop.ReprocessPod(volumeutil.GetUniquePodName(freshPod))
	}
	freshPass := 0
	if freshPod != nil && len(retryPods) > 1 {
		freshPass = 1
		denials := 0
		// Inject ordinary work from the first retry response, after the current
		// snapshot was taken, rather than only between completed passes.
		transport.onRead = func(read nodeGraphVolumeRead) {
			if read.status == http.StatusForbidden {
				denials++
				if denials == len(retryPods)+1 {
					addFresh()
				}
			}
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		pop.Run(ctx, config.NewSourcesReady(func(sets.Set[string]) bool { return true }))
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitNodeGraphSignal(t, ctx, pods.entered, "the first volume-populator pass")
	pass := func() {
		select {
		case pods.proceed <- struct{}{}:
		case <-ctx.Done():
			t.Fatalf("releasing volume-populator pass: %v", ctx.Err())
		}
		// The next GetPods call is a barrier: the previous pass has finished
		// publishing all of its results, including errors that were later cleared.
		waitNodeGraphSignal(t, ctx, pods.entered, "completion of the volume-populator pass")
	}
	firstDenial := clock.Now()
	pass()
	expectedDenial := []nodeGraphVolumeRead{{"PVC", http.StatusForbidden}}
	if pvcReplica != pvReplica {
		expectedDenial = []nodeGraphVolumeRead{{"PVC", http.StatusOK}, {"PV", http.StatusForbidden}}
	}
	var initialReads []nodeGraphVolumeRead
	for range retryPods {
		initialReads = append(initialReads, expectedDenial...)
	}
	require.Equal(t, initialReads, transport.snapshot())
	require.Empty(t, dsw.GetVolumesToMount())
	require.Empty(t, dsw.snapshot(), "a transient real NodeAuthorizer denial escaped to volume waiters")

	if options.denyRetryCapacity {
		clock.Step(200 * time.Millisecond)
		pass()
		require.Equal(t, initialReads, transport.snapshot(), "an unadmitted retry must not send an HTTP request")
		for podName := range retryNames {
			errs := dsw.PopPodErrors(podName)
			require.NotEmpty(t, errs)
			for _, err := range errs {
				require.ErrorContains(t, err, "no relationship found")
				require.NotContains(t, err.Error(), "rate limiter")
				started, deferred := kubecontainer.LatestBackoffStart(err)
				require.True(t, deferred, "capacity refusal must retain the original failure time")
				require.Equal(t, firstDenial, started)
			}
		}
		limiter.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(100, 100)
		exhaust = true
	}

	expectedReads := append([]nodeGraphVolumeRead(nil), initialReads...)
	if freshPod != nil && freshPass == 0 {
		addFresh()
	}
	steps := []time.Duration{200 * time.Millisecond, 300 * time.Millisecond, 500 * time.Millisecond, time.Second}
	for i := 0; i < blockedRetries; i++ {
		clock.Step(steps[i])
		pass()
		if i == freshPass && freshPod != nil {
			expectedReads = append(expectedReads, nodeGraphVolumeRead{"PVC", http.StatusOK}, nodeGraphVolumeRead{"PV", http.StatusOK})
			volumes := dsw.GetVolumesToMount()
			require.Len(t, volumes, 1)
			require.Equal(t, volumeutil.GetUniquePodName(freshPod), volumes[0].PodName)
			require.Equal(t, freshPod.Namespace+"-pv", volumes[0].VolumeSpec.PersistentVolume.Name)
		}
		expectedReads = append(expectedReads, expectedDenial...)
		require.Equal(t, expectedReads, transport.snapshot(), "each pass must admit fresh work and perform at most one retry-Pod lookup pass")
		if i < 3 {
			require.Empty(t, dsw.snapshot(), "an eligible retry must not publish the temporary denial")
		}
	}
	if exhaust {
		clock.Step(time.Hour)
		pass()
		require.Equal(t, expectedReads, transport.snapshot(), "exhaustion must not issue another fast lookup")
		require.NotEmpty(t, dsw.snapshot(), "exhaustion must surface the denied lookup")
		for _, err := range dsw.snapshot() {
			require.Contains(t, err.message, "no relationship found")
			require.Equal(t, firstDenial, err.backoffStart, "a stalled pass must not move the fallback origin")
		}
		previouslyPublished := len(dsw.snapshot())
		reprocess()
		pass()
		expectedReads = append(expectedReads, initialReads...)
		require.Equal(t, expectedReads, transport.snapshot())
		for _, err := range dsw.snapshot()[previouslyPublished:] {
			require.True(t, err.backoffStart.IsZero(), "ordinary resync must not reuse startup timing")
		}
		for podName := range retryNames {
			for _, err := range dsw.PopPodErrors(podName) {
				_, deferred := kubecontainer.LatestBackoffStart(err)
				require.False(t, deferred, "a fresh occurrence must replace cached startup timing")
			}
		}
		published := len(dsw.snapshot())
		clock.Step(time.Hour)
		pass()
		require.Len(t, transport.snapshot(), len(expectedReads), "ordinary resync must not renew the fast-retry budget")
		require.Len(t, dsw.snapshot(), published)
	}

	gate.release()
	node := pvReplica.nodeClient(t, "node1")
	waitNodeGraphAccess(t, ctx, func(ctx context.Context) error {
		_, err := node.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		return err
	})
	recoveryPasses := len(retryPods)
	if exhaust {
		reprocess()
		recoveryPasses = 1
	} else {
		clock.Step(time.Second)
	}
	for i := 0; i < recoveryPasses; i++ {
		pass()
		clock.Step(100 * time.Millisecond)
	}
	volumes := dsw.GetVolumesToMount()
	expectedVolumes := len(retryPods)
	if freshPod != nil {
		expectedVolumes++
	}
	require.Len(t, volumes, expectedVolumes)
	found := sets.New[volumetypes.UniquePodName]()
	for _, volume := range volumes {
		if retryNames.Has(volume.PodName) {
			found.Insert(volume.PodName)
			require.Equal(t, pv.UID, volume.VolumeSpec.PersistentVolume.UID)
		} else {
			require.NotNil(t, freshPod)
			require.Equal(t, volumeutil.GetUniquePodName(freshPod), volume.PodName)
		}
	}
	require.Equal(t, retryNames, found, "every originally denied Pod must still recover")
	require.Empty(t, dsw.GetPodsWithErrors())
	reads := transport.snapshot()
	require.Equal(t, []nodeGraphVolumeRead{{"PVC", http.StatusOK}, {"PV", http.StatusOK}}, reads[len(reads)-2:])
	if !exhaust {
		require.Empty(t, dsw.snapshot(), "even a briefly published error can trigger Pod-sync backoff")
	}
}

func waitNodeGraphAccess(t *testing.T, ctx context.Context, get func(context.Context) error) {
	t.Helper()
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		lastErr = get(ctx)
		if apierrors.IsForbidden(lastErr) {
			return false, nil
		}
		return lastErr == nil, lastErr
	})
	require.NoError(t, err, "node access did not converge: %v", lastErr)
}

func waitNodeGraphSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", description, ctx.Err())
	}
}

func createNodeGraphVolumes(t *testing.T, ctx context.Context, client clientset.Interface, namespace string) (*v1.PersistentVolumeClaim, *v1.PersistentVolume) {
	t.Helper()
	claim, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim"},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: namespace + "-pv", StorageClassName: ptr.To(""),
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	pv, err := client.CoreV1().PersistentVolumes().Create(ctx, &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName},
		Spec: v1.PersistentVolumeSpec{
			Capacity:    claim.Spec.Resources.Requests,
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			ClaimRef:    &v1.ObjectReference{Namespace: namespace, Name: claim.Name, UID: claim.UID},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/node-graph-lag"},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	claim.Status.Phase = v1.ClaimBound
	claim.Status.Capacity = pv.Spec.Capacity
	claim.Status.AccessModes = pv.Spec.AccessModes
	claim, err = client.CoreV1().PersistentVolumeClaims(namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
	require.NoError(t, err)
	pv.Status.Phase = v1.VolumeBound
	pv, err = client.CoreV1().PersistentVolumes().UpdateStatus(ctx, pv, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Equal(t, claim.UID, pv.Spec.ClaimRef.UID)
	require.Equal(t, pv.Name, claim.Spec.VolumeName)
	return claim, pv
}

func TestNodeAuthorizerBindingGraphLag(t *testing.T) {
	runNodeGraphLagCases(t, false)
}

func TestNodeAuthorizerVolumeStartupRetry(t *testing.T) {
	runNodeGraphLagCases(t, true)
}

func runNodeGraphLagCases(t *testing.T, retryVolumes bool) {
	t.Helper()
	tCtx := ktesting.Init(t)
	storage := framework.SharedEtcd()
	replicas := []*nodeGraphReplica{
		startNodeGraphReplica(t, tCtx, storage),
		startNodeGraphReplica(t, tCtx, storage),
	}
	for _, name := range []string{"node1", "node2"} {
		_, err := replicas[0].client.CoreV1().Nodes().Create(tCtx, &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	type testCase struct {
		name              string
		held              int
		blockedRetries    int
		exhaust           bool
		freshWork         bool
		retryCount        int
		denyRetryCapacity bool
	}
	cases := []testCase{{name: "same_replica", held: 0}, {name: "cross_replica", held: 1}}
	if retryVolumes {
		cases = append(cases,
			testCase{name: "same_replica_delayed_convergence", held: 0, blockedRetries: 3},
			testCase{name: "cross_replica_delayed_convergence", held: 1, blockedRetries: 3},
			testCase{name: "same_replica_budget_exhausted", held: 0, exhaust: true},
			testCase{name: "cross_replica_budget_exhausted", held: 1, exhaust: true},
			testCase{name: "same_replica_attempts_exhausted", held: 0, blockedRetries: 4, exhaust: true},
			testCase{name: "cross_replica_attempts_exhausted", held: 1, blockedRetries: 4, exhaust: true},
			testCase{name: "same_replica_fresh_work_priority", held: 0, blockedRetries: 1, freshWork: true},
			testCase{name: "cross_replica_fresh_work_priority", held: 1, blockedRetries: 1, freshWork: true},
			testCase{name: "same_replica_retry_quantum", held: 0, blockedRetries: 2, freshWork: true, retryCount: 3},
			testCase{name: "cross_replica_retry_quantum", held: 1, blockedRetries: 2, freshWork: true, retryCount: 3},
			testCase{name: "same_replica_retry_capacity", held: 0, denyRetryCapacity: true},
			testCase{name: "cross_replica_retry_capacity", held: 1, denyRetryCapacity: true},
		)
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(tCtx, time.Minute)
			defer cancel()
			namespace := fmt.Sprintf("node-graph-%d", index)
			_, err := replicas[0].client.CoreV1().Namespaces().Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
			require.NoError(t, err)
			clients := []clientset.Interface{replicas[0].nodeClient(t, "node1"), replicas[1].nodeClient(t, "node1")}

			// Establish that this is an already recognized node, not an identity/bootstrap race.
			_, err = replicas[0].client.CoreV1().ConfigMaps(namespace).Create(ctx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "anchor"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = replicas[0].client.CoreV1().Pods(namespace).Create(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "anchor"},
				Spec: v1.PodSpec{
					NodeName: "node1",
					Containers: []v1.Container{{
						Name: "pause", Image: "registry.k8s.io/pause:3.10",
						VolumeMounts: []v1.VolumeMount{{Name: "anchor", MountPath: "/anchor"}},
					}},
					Volumes: []v1.Volume{{
						Name: "anchor",
						VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{
							LocalObjectReference: v1.LocalObjectReference{Name: "anchor"},
						}},
					}},
				},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
			for _, client := range clients {
				waitNodeGraphAccess(t, ctx, func(ctx context.Context) error {
					_, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, "anchor", metav1.GetOptions{})
					return err
				})
			}

			claim, pv := createNodeGraphVolumes(t, ctx, replicas[0].client, namespace)
			podTemplate := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name: "pause", Image: "registry.k8s.io/pause:3.10",
						VolumeMounts: []v1.VolumeMount{{Name: "volume", MountPath: "/scratch"}},
					}},
					Volumes: []v1.Volume{{
						Name: "volume",
						VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: claim.Name,
						}},
					}},
				},
			}
			var freshPod *v1.Pod
			if tc.freshWork {
				freshNamespace := namespace + "-fresh"
				_, err = replicas[0].client.CoreV1().Namespaces().Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: freshNamespace}}, metav1.CreateOptions{})
				require.NoError(t, err)
				freshClaim, freshPV := createNodeGraphVolumes(t, ctx, replicas[0].client, freshNamespace)
				freshTemplate := podTemplate.DeepCopy()
				freshTemplate.Name = "fresh"
				freshTemplate.Spec.NodeName = "node1"
				freshTemplate.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = freshClaim.Name
				freshPod, err = replicas[0].client.CoreV1().Pods(freshNamespace).Create(ctx, freshTemplate, metav1.CreateOptions{})
				require.NoError(t, err)
				// Converge this independent relationship before blocking the shared
				// Pod handler; delivery to the populator is delayed until its retry.
				for _, client := range clients {
					waitNodeGraphAccess(t, ctx, func(ctx context.Context) error {
						_, err := client.CoreV1().PersistentVolumes().Get(ctx, freshPV.Name, metav1.GetOptions{})
						return err
					})
					_, err = client.CoreV1().PersistentVolumeClaims(freshNamespace).Get(ctx, freshClaim.Name, metav1.GetOptions{})
					require.NoError(t, err)
				}
			}
			gates := []*nodeGraphPodGate{
				replicas[0].gates.add(t, namespace, "pod", tc.held == 0),
				replicas[1].gates.add(t, namespace, "pod", tc.held == 1),
			}
			pod, err := replicas[0].client.CoreV1().Pods(namespace).Create(ctx, podTemplate, metav1.CreateOptions{})
			require.NoError(t, err)
			for _, gate := range gates {
				waitNodeGraphSignal(t, ctx, gate.unassigned, "the unassigned Pod notification")
				require.Equal(t, pod.UID, gate.uid)
			}
			podWatch, err := clients[tc.held].CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
				FieldSelector:   fields.OneTermEqualSelector("spec.nodeName", "node1").String(),
				ResourceVersion: pod.ResourceVersion,
			})
			require.NoError(t, err)
			defer podWatch.Stop()
			require.NoError(t, replicas[0].client.CoreV1().Pods(namespace).Bind(ctx, &v1.Binding{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID},
				Target:     v1.ObjectReference{APIVersion: "v1", Kind: "Node", Name: "node1"},
			}, metav1.CreateOptions{}))
			waitNodeGraphSignal(t, ctx, gates[tc.held].blocked, "the held bound-Pod graph notification")
			cachedPod, err := replicas[tc.held].pods.Lister().Pods(namespace).Get(pod.Name)
			require.NoError(t, err)
			require.Equal(t, pod.UID, cachedPod.UID)
			require.Equal(t, "node1", cachedPod.Spec.NodeName)

			observed := false
			for !observed {
				select {
				case event, open := <-podWatch.ResultChan():
					require.True(t, open, "node Pod watch closed before delivering the binding")
					if event.Type == watch.Error {
						t.Fatalf("node Pod watch failed: %v", apierrors.FromObject(event.Object))
					}
					if watched, ok := event.Object.(*v1.Pod); ok && watched.UID == pod.UID {
						require.Equal(t, "node1", watched.Spec.NodeName)
						pod = watched.DeepCopy()
						observed = true
					}
				case <-ctx.Done():
					t.Fatalf("binding was not delivered to the node's watch: %v", ctx.Err())
				}
			}

			fast := clients[1-tc.held]
			waitNodeGraphAccess(t, ctx, func(ctx context.Context) error {
				_, err := fast.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				return err
			})
			_, err = fast.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			for _, get := range []func() error{
				func() error {
					_, err := clients[tc.held].CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claim.Name, metav1.GetOptions{})
					return err
				},
				func() error {
					_, err := clients[tc.held].CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
					return err
				},
			} {
				err := get()
				require.True(t, apierrors.IsForbidden(err), "expected a real authorization denial, got %v", err)
				require.ErrorContains(t, err, "no relationship found")
			}
			if retryVolumes {
				retryPods := []*v1.Pod{pod}
				for i := 1; i < tc.retryCount; i++ {
					template := podTemplate.DeepCopy()
					template.Name = fmt.Sprintf("retry-%d", i)
					template.Spec.NodeName = "node1"
					additional, err := replicas[0].client.CoreV1().Pods(namespace).Create(ctx, template, metav1.CreateOptions{})
					require.NoError(t, err)
					retryPods = append(retryPods, additional)
				}
				pvcReplica := replicas[tc.held]
				if tc.held == 1 {
					pvcReplica = replicas[0]
				}
				testNodeGraphVolumeRetry(t, ctx, retryPods, pv, pvcReplica, replicas[tc.held], gates[tc.held], nodeGraphVolumeRetryOptions{
					blockedRetries: tc.blockedRetries, exhaust: tc.exhaust, freshPod: freshPod, denyRetryCapacity: tc.denyRetryCapacity,
				})
			}
			gates[tc.held].release()
			waitNodeGraphAccess(t, ctx, func(ctx context.Context) error {
				_, err := clients[tc.held].CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				return err
			})
			_, err = clients[tc.held].CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			for _, replica := range replicas {
				other := replica.nodeClient(t, "node2")
				_, err = other.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claim.Name, metav1.GetOptions{})
				require.True(t, apierrors.IsForbidden(err), "unrelated node gained PVC access: %v", err)
				_, err = other.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
				require.True(t, apierrors.IsForbidden(err), "unrelated node gained PV access: %v", err)
			}
			t.Logf("%s: binding and node watch completed while replica %d denied PVC/PV access; releasing only its Pod notification restored access", tc.name, tc.held)
		})
	}
}
