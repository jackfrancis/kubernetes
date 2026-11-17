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

package kubelet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
	kubeletvolume "k8s.io/kubernetes/pkg/kubelet/volumemanager"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

const volumeRetryDenial = "volume retry experiment: authorization lag"

// rate.Wait does not expose a typed error for deadline-preflight rejection.
const volumeRetryDeadlineRejection = "rate: Wait(n=1) would exceed context deadline"

type volumeRetryScenario struct {
	name                                         string
	healthy, transient, permanent, late, volumes int
	resource                                     string
	lag, latency                                 time.Duration
	qps                                          float32
	burst                                        int
	headroom                                     *volumeRetryHeadroomPolicy
	trace                                        bool
}

type volumeRetryHeadroomPolicy struct {
	name           string
	percent        int
	yieldToWaiters bool
	wholePod       bool
	observeWait    bool
	contentionVeto bool
}

type volumeRetryPod struct {
	pod                                           *v1.Pod
	group                                         string
	submitted, firstDenial, authorized, succeeded time.Time
	firstSyncError                                time.Time
	gets, denials, syncAttempts, syncErrors       int
	rateDeadlineErrors                            int
}

type volumeRetryObject struct {
	object   runtime.Object
	owner    *volumeRetryPod
	resource string
	name     string
}

type volumeRetryAPI struct {
	mu          sync.Mutex
	scenario    volumeRetryScenario
	clock       clock.PassiveClock
	pods        []*volumeRetryPod
	objects     map[string]volumeRetryObject
	active      int
	pvcGETs     int
	pvGETs      int
	unexpected  []string
	trace       *volumeRetryTokenTrace
	lateArrival time.Time
}

func newVolumeRetryAPI(s volumeRetryScenario, c clock.PassiveClock) *volumeRetryAPI {
	api := &volumeRetryAPI{scenario: s, clock: c, objects: make(map[string]volumeRetryObject)}
	if s.trace {
		api.trace = &volumeRetryTokenTrace{started: time.Now()}
	}
	for _, group := range []struct {
		name  string
		count int
	}{{"permanent", s.permanent}, {"transient", s.transient}, {"healthy", s.healthy}, {"late", s.late}} {
		for range group.count {
			name := fmt.Sprintf("pod-%04d", len(api.pods))
			pod := podWithUIDNameNsSpec(types.UID(name), name, "volume-retry-test", v1.PodSpec{
				NodeName: testKubeletHostname,
				Containers: []v1.Container{{
					Name: "container", Image: "fake-image",
				}},
			})
			entry := &volumeRetryPod{pod: pod, group: group.name}
			api.pods = append(api.pods, entry)
			for i := range s.volumes {
				volumeName := fmt.Sprintf("volume-%d", i)
				claimName := name + "-" + volumeName
				pvName := "pv-" + claimName
				claim := &v1.PersistentVolumeClaim{
					TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
					ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: pod.Namespace, UID: types.UID(claimName)},
					Spec: v1.PersistentVolumeClaimSpec{
						VolumeName:  pvName,
						VolumeMode:  ptr.To(v1.PersistentVolumeFilesystem),
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
						Resources: v1.VolumeResourceRequirements{
							Requests: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")},
						},
					},
					Status: v1.PersistentVolumeClaimStatus{
						Phase: v1.ClaimBound, Capacity: v1.ResourceList{v1.ResourceStorage: resource.MustParse("1Mi")},
					},
				}
				pv := &v1.PersistentVolume{
					TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
					ObjectMeta: metav1.ObjectMeta{Name: pvName},
					Spec: v1.PersistentVolumeSpec{
						ClaimRef:    &v1.ObjectReference{Namespace: claim.Namespace, Name: claim.Name, UID: claim.UID},
						VolumeMode:  ptr.To(v1.PersistentVolumeFilesystem),
						AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
						Capacity:    claim.Spec.Resources.Requests,
						PersistentVolumeSource: v1.PersistentVolumeSource{
							HostPath: &v1.HostPathVolumeSource{Path: "/fake-volume/" + claimName},
						},
					},
				}
				pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
					},
				})
				pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
					v1.VolumeMount{Name: volumeName, MountPath: fmt.Sprintf("/mnt/%d", i)})
				api.objects["/api/v1/namespaces/"+pod.Namespace+"/persistentvolumeclaims/"+claimName] =
					volumeRetryObject{claim, entry, "persistentvolumeclaims", claimName}
				api.objects["/api/v1/persistentvolumes/"+pvName] =
					volumeRetryObject{pv, entry, "persistentvolumes", pvName}
			}
		}
	}
	return api
}

func (a *volumeRetryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	entry, found := a.objects[r.URL.Path]
	if r.Method != http.MethodGet || !found {
		a.mu.Lock()
		a.unexpected = append(a.unexpected, r.Method+" "+r.URL.Path)
		a.mu.Unlock()
		http.Error(w, "unexpected experiment request", http.StatusNotFound)
		return
	}
	requestID := 0
	if a.trace != nil {
		var err error
		requestID, err = a.trace.apiRequest(r, entry)
		if err != nil {
			a.mu.Lock()
			a.unexpected = append(a.unexpected, err.Error())
			a.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	a.mu.Lock()
	entry.owner.gets++
	a.active++
	if entry.resource == "persistentvolumeclaims" {
		a.pvcGETs++
	} else {
		a.pvGETs++
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active--
		a.mu.Unlock()
	}()
	if a.scenario.latency > 0 {
		timer := time.NewTimer(a.scenario.latency)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			if a.trace != nil {
				a.trace.record(volumeRetryTraceEvent{Kind: "api_canceled", RequestID: requestID, Reason: r.Context().Err().Error()})
			}
			return
		}
	}

	a.mu.Lock()
	pod := entry.owner
	denied := false
	if entry.resource == a.scenario.resource && (pod.group == "transient" || pod.group == "permanent") {
		now := a.clock.Now()
		if pod.firstDenial.IsZero() {
			pod.firstDenial = now
			if pod.group == "transient" {
				pod.authorized = now.Add(a.scenario.lag)
			}
		}
		denied = pod.group == "permanent" || now.Before(pod.authorized)
		if denied {
			pod.denials++
		}
	}
	a.mu.Unlock()

	var response any = entry.object
	code := http.StatusOK
	if denied {
		status := apierrors.NewForbidden(v1.Resource(entry.resource), entry.name, fmt.Errorf("%s", volumeRetryDenial)).ErrStatus
		status.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
		response = status
		code = http.StatusForbidden
	}
	if a.trace != nil {
		a.trace.record(volumeRetryTraceEvent{Kind: "api_response", RequestID: requestID, HTTPStatus: code})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(response); err != nil && r.Context().Err() == nil {
		a.mu.Lock()
		a.unexpected = append(a.unexpected, err.Error())
		a.mu.Unlock()
	}
}

const volumeRetryTraceHeader = "X-Volume-Retry-Trace"

type volumeRetryTraceRequestKey struct{}

type volumeRetryTraceRequest struct {
	ID                int      `json:"id"`
	CreatedMS         float64  `json:"created_ms"`
	Callers           []string `json:"callers"`
	Path              string   `json:"path,omitempty"`
	Pod               string   `json:"pod,omitempty"`
	Group             string   `json:"group,omitempty"`
	Resource          string   `json:"resource,omitempty"`
	OrdinaryWaitCalls int      `json:"ordinary_wait_calls"`
	APIGETs           int      `json:"api_gets"`
}

type volumeRetryTraceEvent struct {
	AtMS         float64  `json:"at_ms"`
	Kind         string   `json:"kind"`
	RequestID    int      `json:"request_id,omitempty"`
	Pod          string   `json:"pod,omitempty"`
	Attempt      int      `json:"attempt,omitempty"`
	Tokens       *float64 `json:"tokens,omitempty"`
	TokensBefore *float64 `json:"tokens_before,omitempty"`
	Pending      *int     `json:"pending,omitempty"`
	Accepted     *bool    `json:"accepted,omitempty"`
	Reason       string   `json:"reason,omitempty"`
	HTTPStatus   int      `json:"http_status,omitempty"`
}

type volumeRetryTraceResult struct {
	Requests []volumeRetryTraceRequest `json:"requests"`
	Events   []volumeRetryTraceEvent   `json:"events"`
}

type volumeRetryTokenTrace struct {
	mu      sync.Mutex
	started time.Time
	volumeRetryTraceResult
}

func (t *volumeRetryTokenTrace) newRequest() int {
	var pcs [32]uintptr
	frames := goruntime.CallersFrames(pcs[:goruntime.Callers(3, pcs[:])])
	var callers []string
	for {
		frame, more := frames.Next()
		if strings.HasPrefix(frame.Function, "k8s.io/kubernetes/") {
			callers = append(callers, frame.Function)
		}
		if !more {
			break
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	id := len(t.Requests) + 1
	t.Requests = append(t.Requests, volumeRetryTraceRequest{
		ID: id, CreatedMS: time.Since(t.started).Seconds() * 1000, Callers: callers,
	})
	return id
}

func (t *volumeRetryTokenTrace) record(event volumeRetryTraceEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	event.AtMS = time.Since(t.started).Seconds() * 1000
	t.Events = append(t.Events, event)
}

func (t *volumeRetryTokenTrace) apiRequest(r *http.Request, object volumeRetryObject) (int, error) {
	id, err := strconv.Atoi(r.Header.Get(volumeRetryTraceHeader))
	if err != nil {
		return 0, fmt.Errorf("volume trace request %s has an invalid request ID: %w", r.URL.Path, err)
	}
	t.mu.Lock()
	if id < 1 || id > len(t.Requests) {
		t.mu.Unlock()
		return 0, fmt.Errorf("volume trace request %s has unknown request ID %d", r.URL.Path, id)
	}
	request := &t.Requests[id-1]
	request.Path = r.URL.Path
	request.Pod = object.owner.pod.Name
	request.Group = object.owner.group
	request.Resource = object.resource
	request.APIGETs++
	t.mu.Unlock()
	t.record(volumeRetryTraceEvent{Kind: "api_get", RequestID: id})
	return id, nil
}

func (t *volumeRetryTokenTrace) result() *volumeRetryTraceResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	return &volumeRetryTraceResult{Requests: slices.Clone(t.Requests), Events: slices.Clone(t.Events)}
}

type volumeRetryTraceLimiter struct {
	flowcontrol.RateLimiter
	trace     *volumeRetryTokenTrace
	requestID int
}

func (l volumeRetryTraceLimiter) Wait(ctx context.Context) error {
	l.trace.mu.Lock()
	l.trace.Requests[l.requestID-1].OrdinaryWaitCalls++
	l.trace.mu.Unlock()
	return l.RateLimiter.Wait(context.WithValue(ctx, volumeRetryTraceRequestKey{}, l.requestID))
}

type volumeRetryTraceRESTClient struct {
	rest.Interface
	trace *volumeRetryTokenTrace
}

func (c volumeRetryTraceRESTClient) Get() *rest.Request {
	id := c.trace.newRequest()
	// A request-local retry throttle may replace this wait wrapper, but the ID
	// survives so actual HTTP requests remain attributable without deadline guesses.
	return c.Interface.Get().
		SetHeader(volumeRetryTraceHeader, strconv.Itoa(id)).
		Throttle(volumeRetryTraceLimiter{c.GetRateLimiter(), c.trace, id})
}

type volumeRetryTraceClient struct {
	clientset.Interface
	core corev1client.CoreV1Interface
}

func (c volumeRetryTraceClient) CoreV1() corev1client.CoreV1Interface { return c.core }

type volumeRetryPodPreflightDecision struct {
	AtMS           float64   `json:"at_ms"`
	Pod            string    `json:"pod"`
	UID            types.UID `json:"uid"`
	CostTokens     int       `json:"cost_tokens"`
	RequiredTokens int       `json:"required_tokens"`
	Tokens         float64   `json:"tokens"`
	Pending        int       `json:"pending"`
	Admitted       bool      `json:"admitted"`
	Reason         string    `json:"reason,omitempty"`
}

type volumeRetryPodPreflightResult struct {
	HookInstalled bool                              `json:"hook_installed"`
	Calls         int                               `json:"calls"`
	Admitted      int                               `json:"admitted"`
	Rejected      int                               `json:"rejected"`
	Decisions     []volumeRetryPodPreflightDecision `json:"decisions"`
}

type volumeRetryPodPreflightClient struct {
	clientset.Interface
	limiter *volumeRetryLimiter
}

func (c volumeRetryPodPreflightClient) RegisterInitialVolumeRetryPreflight() {
	c.limiter.mu.Lock()
	defer c.limiter.mu.Unlock()
	c.limiter.preflight.HookInstalled = true
}

func (c volumeRetryPodPreflightClient) AdmitInitialVolumeRetry(ctx context.Context, pod *v1.Pod) bool {
	return c.limiter.admitPodRetry(ctx, pod)
}

type volumeRetryContentionEpisode struct {
	UID   types.UID
	Epoch uint64
}

type volumeRetryContentionCheck struct {
	AtMS         float64   `json:"at_ms"`
	Phase        string    `json:"phase"`
	UID          types.UID `json:"uid"`
	EpisodeEpoch uint64    `json:"episode_epoch"`
	CurrentEpoch uint64    `json:"current_epoch"`
	Vetoed       bool      `json:"vetoed"`
}

type volumeRetryContentionResult struct {
	HookInstalled bool                         `json:"hook_installed"`
	VetoEnabled   bool                         `json:"veto_enabled"`
	Epoch         uint64                       `json:"epoch"`
	Queued        []volumeRetryQueuedWait      `json:"queued"`
	Checks        []volumeRetryContentionCheck `json:"checks"`
}

type volumeRetryContentionClient struct{ volumeRetryPodPreflightClient }

func (c volumeRetryContentionClient) RegisterInitialVolumeRetryContention() {
	c.limiter.mu.Lock()
	defer c.limiter.mu.Unlock()
	c.limiter.contention.HookInstalled = true
}

func (c volumeRetryContentionClient) InitialVolumeRetryContentionEpoch() uint64 {
	return c.limiter.observer.contentionEpoch()
}

func (c volumeRetryContentionClient) AdmitInitialVolumeRetryAfterContention(_ context.Context, pod *v1.Pod, epoch uint64) bool {
	c.limiter.mu.Lock()
	defer c.limiter.mu.Unlock()
	current := c.limiter.observer.contentionEpoch()
	return !c.limiter.recordContentionCheck("pass", volumeRetryContentionEpisode{pod.UID, epoch}, current)
}

type volumeRetryContentionLimiter struct{ *volumeRetryLimiter }

func (l volumeRetryContentionLimiter) TryAcceptInitialVolumeRetry(uid types.UID, epoch uint64) bool {
	return l.volumeRetryLimiter.tryAccept(&volumeRetryContentionEpisode{uid, epoch})
}

func volumeRetryPodCost(pod *v1.Pod) (int, error) {
	if pod == nil {
		return 0, fmt.Errorf("whole-Pod retry preflight requires a Pod")
	}
	mounts, devices, _ := volumeutil.GetPodVolumeNames(pod, false)
	cost := 0
	for _, podVolume := range pod.Spec.Volumes {
		if !mounts.Has(podVolume.Name) && !devices.Has(podVolume.Name) {
			continue
		}
		if devices.Has(podVolume.Name) || podVolume.PersistentVolumeClaim == nil {
			return 0, fmt.Errorf("whole-Pod retry preflight cannot estimate volume %q for Pod %s/%s: the cost model supports only filesystem PVCs",
				podVolume.Name, pod.Namespace, pod.Name)
		}
		// This filesystem fixture needs PVC/PV lookups plus a later claim-size
		// lookup; attachable and resize-capable paths need a different cost model.
		cost += 3
	}
	if cost == 0 {
		return 0, fmt.Errorf("whole-Pod retry preflight requires a used filesystem PVC for Pod %s/%s", pod.Namespace, pod.Name)
	}
	return cost, nil
}

type volumeRetryLimiter struct {
	flowcontrol.RateLimiter
	lifetime         context.Context
	mu               sync.Mutex
	active           sync.WaitGroup
	pending          int
	closed           bool
	calls            int
	canceled         int
	deadlineRejected int
	tryCalls         int
	tryRejected      int
	tokenBucket      *rate.Limiter
	headroomTokens   int
	yieldToWaiters   bool
	headroomRejected int
	pendingRejected  int
	unexpected       []string
	waited           time.Duration
	trace            *volumeRetryTokenTrace
	started          time.Time
	preflight        *volumeRetryPodPreflightResult
	observer         *volumeRetryObservedWait
	contention       *volumeRetryContentionResult
}

func (l *volumeRetryLimiter) recordContentionCheck(phase string, episode volumeRetryContentionEpisode, current uint64) bool {
	vetoed := l.contention.VetoEnabled && current > episode.Epoch
	if episode.Epoch > current {
		l.unexpected = append(l.unexpected, fmt.Sprintf("volume retry episode %q has future contention epoch %d; current epoch is %d", episode.UID, episode.Epoch, current))
		vetoed = true
	}
	l.contention.Checks = append(l.contention.Checks, volumeRetryContentionCheck{
		AtMS: time.Since(l.started).Seconds() * 1000, Phase: phase, UID: episode.UID,
		EpisodeEpoch: episode.Epoch, CurrentEpoch: current, Vetoed: vetoed,
	})
	return vetoed
}

func (l *volumeRetryLimiter) admitPodRetry(ctx context.Context, pod *v1.Pod) bool {
	cost, err := volumeRetryPodCost(pod)
	l.mu.Lock()
	defer l.mu.Unlock()
	decision := volumeRetryPodPreflightDecision{
		AtMS:       time.Since(l.started).Seconds() * 1000,
		CostTokens: cost, RequiredTokens: cost + l.headroomTokens,
		Tokens: l.tokenBucket.Tokens(), Pending: l.pending,
	}
	if pod != nil {
		decision.Pod, decision.UID = pod.Name, pod.UID
	}
	switch {
	case err != nil:
		decision.Reason = "unsupported"
		l.unexpected = append(l.unexpected, err.Error())
	case l.closed:
		decision.Reason = "closed"
	case ctx.Err() != nil || l.lifetime.Err() != nil:
		decision.Reason = "canceled"
	case l.pending > 0:
		decision.Reason = "pending"
	case decision.Tokens < float64(decision.RequiredTokens):
		decision.Reason = "capacity"
	default:
		decision.Admitted = true
	}
	l.preflight.Calls++
	if decision.Admitted {
		l.preflight.Admitted++
	} else {
		l.preflight.Rejected++
	}
	l.preflight.Decisions = append(l.preflight.Decisions, decision)
	return decision.Admitted
}

func (l *volumeRetryLimiter) recordTrace(event volumeRetryTraceEvent) {
	if l.trace != nil {
		event.Tokens = ptr.To(l.tokenBucket.Tokens())
		event.Pending = ptr.To(l.pending)
		l.trace.record(event)
	}
}

func (l *volumeRetryLimiter) traceSnapshot(event volumeRetryTraceEvent) {
	if l.trace != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.recordTrace(event)
	}
}

// Use the same engine as client-go, exposing tokens only inside this harness.
type volumeRetryTokenBucket struct {
	*rate.Limiter
}

func (l volumeRetryTokenBucket) TryAccept() bool { return l.Allow() }
func (l volumeRetryTokenBucket) Stop()           {}
func (l volumeRetryTokenBucket) QPS() float32    { return float32(l.Limit()) }
func (l volumeRetryTokenBucket) Accept() {
	now := time.Now()
	time.Sleep(l.ReserveN(now, 1).DelayFrom(now))
}

type volumeRetryQueuedWait struct {
	Epoch     uint64  `json:"epoch"`
	AtMS      float64 `json:"at_ms"`
	DelayMS   float64 `json:"delay_ms"`
	RequestID int     `json:"request_id,omitempty"`
}

type volumeRetryObservedWait struct {
	*rate.Limiter
	mu      sync.Mutex
	started time.Time
	epoch   uint64
	queued  []volumeRetryQueuedWait
}

func (l *volumeRetryObservedWait) Stop()        {}
func (l *volumeRetryObservedWait) QPS() float32 { return float32(l.Limit()) }

func (l *volumeRetryObservedWait) TryAccept() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Allow()
}

func (l *volumeRetryObservedWait) Accept() {
	now := time.Now()
	l.mu.Lock()
	reservation := l.ReserveN(now, 1)
	l.mu.Unlock()
	time.Sleep(reservation.DelayFrom(now))
}

func (l *volumeRetryObservedWait) contentionEpoch() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

func (l *volumeRetryObservedWait) tryAcceptSince(epoch uint64, veto bool) (bool, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if epoch > l.epoch || veto && l.epoch > epoch {
		return false, l.epoch
	}
	return l.Allow(), l.epoch
}

func (l *volumeRetryObservedWait) reserveWait(ctx context.Context, now time.Time) (*rate.Reservation, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	limit, burst := l.Limit(), l.Burst()
	if burst < 1 && limit != rate.Inf {
		return nil, 0, fmt.Errorf("rate: Wait(n=1) exceeds limiter's burst %d", burst)
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	// Serialize all token mutations so deadline preflight can inspect the same
	// balance that ReserveN will use, without making and undoing a reservation.
	delay := time.Duration(0)
	if missing := 1 - l.TokensAt(now); missing > 0 && limit != rate.Inf {
		delay = rate.InfDuration
		if limit > 0 {
			nanos := missing / float64(limit) * float64(time.Second)
			if nanos <= float64(math.MaxInt64) {
				delay = time.Duration(nanos)
			}
		}
	}
	if deadline, ok := ctx.Deadline(); ok && limit != rate.Inf && delay > deadline.Sub(now) {
		return nil, 0, fmt.Errorf("%s", volumeRetryDeadlineRejection)
	}
	reservation := l.ReserveN(now, 1)
	if !reservation.OK() {
		return nil, 0, fmt.Errorf("instrumented volume limiter could not reserve one token after successful deadline preflight")
	}
	delay = reservation.DelayFrom(now)
	if delay > 0 {
		l.epoch++
		requestID, _ := ctx.Value(volumeRetryTraceRequestKey{}).(int)
		l.queued = append(l.queued, volumeRetryQueuedWait{
			Epoch: l.epoch, AtMS: time.Since(l.started).Seconds() * 1000,
			DelayMS: delay.Seconds() * 1000, RequestID: requestID,
		})
	}
	return reservation, delay, nil
}

func (l *volumeRetryObservedWait) Wait(ctx context.Context) error {
	reservation, delay, err := l.reserveWait(ctx, time.Now())
	if err != nil || delay == 0 {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		reservation.CancelAt(time.Now())
		l.mu.Unlock()
		return ctx.Err()
	}
}

func (l *volumeRetryLimiter) TryAccept() bool {
	return l.tryAccept(nil)
}

func (l *volumeRetryLimiter) tryAccept(episode *volumeRetryContentionEpisode) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.lifetime.Err() != nil {
		return false
	}
	l.tryCalls++
	accepted := false
	reason := ""
	if l.trace != nil {
		before := l.tokenBucket.Tokens()
		defer func() {
			l.recordTrace(volumeRetryTraceEvent{
				Kind: "retry_admission", TokensBefore: &before, Accepted: ptr.To(accepted), Reason: reason,
			})
		}()
	}
	// Wait registers pending work under this same lock before touching the
	// bucket, so headroom admission cannot race an ordinary reservation.
	if l.yieldToWaiters && l.pending > 0 {
		l.tryRejected++
		l.pendingRejected++
		reason = "pending"
		return false
	}
	if l.headroomTokens > 0 && l.tokenBucket.Tokens() < float64(l.headroomTokens+1) {
		l.tryRejected++
		l.headroomRejected++
		reason = "headroom"
		return false
	}
	if episode != nil {
		var current uint64
		accepted, current = l.observer.tryAcceptSince(episode.Epoch, l.contention.VetoEnabled)
		if l.recordContentionCheck("request", *episode, current) {
			reason = "contention"
		}
	} else {
		accepted = l.RateLimiter.TryAccept()
	}
	if !accepted {
		l.tryRejected++
		if reason == "" {
			reason = "tokens"
		}
	}
	return accepted
}

func (l *volumeRetryLimiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	if l.closed || l.lifetime.Err() != nil {
		l.mu.Unlock()
		return context.Canceled
	}
	requestID := 0
	if l.trace != nil {
		var ok bool
		requestID, ok = ctx.Value(volumeRetryTraceRequestKey{}).(int)
		if !ok || requestID < 1 {
			l.mu.Unlock()
			return fmt.Errorf("volume trace ordinary wait is missing its request ID")
		}
	}
	l.active.Add(1)
	l.pending++
	l.recordTrace(volumeRetryTraceEvent{Kind: "ordinary_wait_start", RequestID: requestID})
	l.mu.Unlock()
	defer l.active.Done()
	ctx, cancel := volumeRetryRequestContext(l.lifetime, ctx)
	defer cancel()
	started := time.Now()
	err := l.RateLimiter.Wait(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending--
	l.calls++
	l.waited += time.Since(started)
	switch {
	case err == nil:
	case err.Error() == volumeRetryDeadlineRejection:
		l.deadlineRejected++
	case ctx.Err() != nil:
		l.canceled++
	default:
		l.unexpected = append(l.unexpected, err.Error())
	}
	event := volumeRetryTraceEvent{Kind: "ordinary_wait_end", RequestID: requestID}
	if err != nil {
		event.Reason = err.Error()
	}
	l.recordTrace(event)
	return err
}

func (l *volumeRetryLimiter) observedDeadlineRejection(err error) bool {
	if err == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.deadlineRejected > 0 &&
		strings.Contains(err.Error(), "client rate limiter Wait returned an error: "+volumeRetryDeadlineRejection)
}

func (l *volumeRetryLimiter) close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.active.Wait()
}

// Some mount-verification lookups use context.TODO(). Scope those requests and
// limiter waits to the experiment too, so no work leaks into the next sample.
// Derive from the request so its deadline and values remain visible to the client.
func volumeRetryRequestContext(lifetime, request context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(request)
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
	}
}

type volumeRetryTransport struct {
	http.RoundTripper
	lifetime context.Context
}

func (t volumeRetryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := volumeRetryRequestContext(t.lifetime, request.Context())
	response, err := t.RoundTripper.RoundTrip(request.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = volumeRetryResponseBody{response.Body, cancel}
	return response, nil
}

type volumeRetryResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b volumeRetryResponseBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

func newVolumeRetryClient(host string, lifetime context.Context, qps float32, burst int, policy *volumeRetryHeadroomPolicy, trace *volumeRetryTokenTrace) (clientset.Interface, *volumeRetryLimiter, error) {
	if trace != nil && policy == nil {
		return nil, nil, fmt.Errorf("volume token tracing requires an observable headroom policy")
	}
	limiter := &volumeRetryLimiter{
		RateLimiter: flowcontrol.NewTokenBucketRateLimiter(qps, burst),
		lifetime:    lifetime,
		trace:       trace,
		started:     time.Now(),
	}
	if policy != nil {
		if policy.observeWait && !policy.wholePod || policy.contentionVeto && !policy.observeWait {
			return nil, nil, fmt.Errorf("invalid contention policy %q: observed waits require whole-Pod preflight and the veto requires observed waits", policy.name)
		}
		if policy.percent < 0 || policy.percent > 100 || ((policy.percent > 0 || policy.wholePod) && !policy.yieldToWaiters) {
			return nil, nil, fmt.Errorf("invalid retry admission policy %q: reserve must be 0-100 percent; reserve and whole-Pod preflight must yield to pending waits", policy.name)
		}
		bucket := rate.NewLimiter(rate.Limit(qps), burst)
		limiter.RateLimiter = volumeRetryTokenBucket{bucket}
		limiter.tokenBucket = bucket
		limiter.headroomTokens = int(math.Ceil(float64(burst) * float64(policy.percent) / 100))
		limiter.yieldToWaiters = policy.yieldToWaiters
		if policy.wholePod {
			limiter.preflight = &volumeRetryPodPreflightResult{}
		}
		if policy.observeWait {
			limiter.observer = &volumeRetryObservedWait{Limiter: bucket, started: limiter.started}
			limiter.RateLimiter = limiter.observer
			limiter.contention = &volumeRetryContentionResult{VetoEnabled: policy.contentionVeto}
		}
	}
	var sharedLimiter flowcontrol.RateLimiter = limiter
	if limiter.observer != nil {
		sharedLimiter = volumeRetryContentionLimiter{limiter}
	}
	client, err := clientset.NewForConfig(&rest.Config{
		Host: host, RateLimiter: sharedLimiter,
		WrapTransport: func(base http.RoundTripper) http.RoundTripper {
			return volumeRetryTransport{base, lifetime}
		},
	})
	if err != nil {
		return nil, nil, err
	}
	var result clientset.Interface = client
	if trace != nil {
		result = volumeRetryTraceClient{client, corev1client.New(volumeRetryTraceRESTClient{client.CoreV1().RESTClient(), trace})}
	}
	if limiter.preflight != nil {
		preflight := volumeRetryPodPreflightClient{result, limiter}
		result = preflight
		if limiter.observer != nil {
			result = volumeRetryContentionClient{preflight}
		}
	}
	return result, limiter, nil
}

type orderedVolumeRetryPods struct{ kubepod.Manager }

func (p orderedVolumeRetryPods) GetPods() []*v1.Pod {
	pods := p.Manager.GetPods()
	// Fix scan order across policies, including failing Pods ahead of late arrivals.
	slices.SortFunc(pods, func(a, b *v1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods
}

// Hide the fake plugin's attach/resize interfaces to model filesystem hostPath volumes.
type volumeRetryFilesystemPlugin struct{ volume.PersistentVolumePlugin }

type volumeRetryPodResult struct {
	Name               string   `json:"name"`
	Group              string   `json:"group"`
	SubmittedMS        float64  `json:"submitted_ms"`
	GETs               int      `json:"gets"`
	Denials            int      `json:"denials"`
	SyncAttempts       int      `json:"sync_attempts"`
	SyncErrors         int      `json:"sync_errors"`
	RateDeadlineErrors int      `json:"rate_deadline_errors"`
	FirstDenialMS      *float64 `json:"first_denial_ms"`
	AuthorizedMS       *float64 `json:"authorized_ms"`
	FirstSyncErrorMS   *float64 `json:"first_sync_error_ms"`
	StartupMS          *float64 `json:"startup_ms"`
	RecoveryMS         *float64 `json:"recovery_ms"`
}

type volumeRetryResult struct {
	Scenario                            string                         `json:"scenario"`
	GoVersion                           string                         `json:"go_version"`
	GOMAXPROCS                          int                            `json:"gomaxprocs"`
	BaseBackoffMS                       float64                        `json:"base_backoff_ms"`
	VolumesPerPod                       int                            `json:"volumes_per_pod"`
	Resource                            string                         `json:"denied_resource"`
	LagMS                               float64                        `json:"lag_ms"`
	APILatencyMS                        float64                        `json:"api_latency_ms"`
	QPS                                 float32                        `json:"qps"`
	Burst                               int                            `json:"burst"`
	ElapsedMS                           float64                        `json:"elapsed_ms"`
	GETs                                int                            `json:"gets"`
	PVCGETs                             int                            `json:"pvc_gets"`
	PVGETs                              int                            `json:"pv_gets"`
	Denials                             int                            `json:"denials"`
	Eligible                            int                            `json:"eligible"`
	Completed                           int                            `json:"completed"`
	Permanent                           int                            `json:"permanent"`
	Incomplete                          int                            `json:"incomplete"`
	LimiterCalls                        int                            `json:"limiter_calls"`
	LimiterCanceled                     int                            `json:"limiter_canceled"`
	LimiterDeadlineRejected             int                            `json:"limiter_deadline_rejected"`
	LimiterWaitMS                       float64                        `json:"limiter_wait_ms"`
	RetryAdmissionCalls                 int                            `json:"retry_admission_calls"`
	RetryAdmissionRejected              int                            `json:"retry_admission_rejected"`
	RetryAdmissionPolicy                string                         `json:"retry_admission_policy"`
	RetryHeadroomTokens                 int                            `json:"retry_headroom_tokens"`
	RetryHeadroomRejected               int                            `json:"retry_headroom_rejected"`
	RetryPendingRejected                int                            `json:"retry_pending_rejected"`
	HealthyP95MS                        *float64                       `json:"healthy_p95_ms"`
	LateHealthyP95MS                    *float64                       `json:"late_healthy_p95_ms"`
	RecoveryP95MS                       *float64                       `json:"recovery_p95_ms"`
	Pods                                []volumeRetryPodResult         `json:"pods"`
	TokenTrace                          *volumeRetryTraceResult        `json:"token_trace,omitempty"`
	PodPreflight                        *volumeRetryPodPreflightResult `json:"pod_preflight,omitempty"`
	ObservedLateArrivalMS               *float64                       `json:"late_arrival_observed_ms,omitempty"`
	TransientCompletedBeforeLateArrival *int                           `json:"transient_completed_before_late_arrival,omitempty"`
	Contention                          *volumeRetryContentionResult   `json:"contention,omitempty"`
}

func volumeRetryPercentile(values []float64, expected int) *float64 {
	// Incomplete groups are censored, not reported as artificially fast successes.
	if expected == 0 || len(values) != expected {
		return nil
	}
	slices.Sort(values)
	value := values[int(math.Ceil(float64(len(values))*0.95))-1]
	return &value
}

func (a *volumeRetryAPI) result(tb testing.TB, baseBackoff, elapsed time.Duration, limiter *volumeRetryLimiter) volumeRetryResult {
	tb.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	require.Zero(tb, a.active, "HTTP handlers must stop before recording final results")
	require.Empty(tb, a.unexpected, "unexpected errors invalidate the experiment")
	s := a.scenario
	result := volumeRetryResult{
		Scenario: s.name, GoVersion: goruntime.Version(), GOMAXPROCS: goruntime.GOMAXPROCS(0),
		BaseBackoffMS: baseBackoff.Seconds() * 1000, VolumesPerPod: s.volumes,
		Resource: s.resource, LagMS: s.lag.Seconds() * 1000, APILatencyMS: s.latency.Seconds() * 1000,
		QPS: s.qps, Burst: s.burst, ElapsedMS: elapsed.Seconds() * 1000,
		Eligible: s.healthy + s.transient + s.late, Permanent: s.permanent,
		PVCGETs: a.pvcGETs, PVGETs: a.pvGETs,
	}
	var healthy, late, recovery []float64
	earlyTransients := 0
	for _, pod := range a.pods {
		item := volumeRetryPodResult{
			Name: pod.pod.Name, Group: pod.group, GETs: pod.gets, Denials: pod.denials,
			SubmittedMS:  pod.submitted.Sub(limiter.started).Seconds() * 1000,
			SyncAttempts: pod.syncAttempts, SyncErrors: pod.syncErrors,
			RateDeadlineErrors: pod.rateDeadlineErrors,
		}
		if !pod.firstDenial.IsZero() {
			value := pod.firstDenial.Sub(pod.submitted).Seconds() * 1000
			item.FirstDenialMS = &value
		}
		if !pod.authorized.IsZero() {
			value := pod.authorized.Sub(pod.submitted).Seconds() * 1000
			item.AuthorizedMS = &value
		}
		if !pod.firstSyncError.IsZero() {
			value := pod.firstSyncError.Sub(pod.submitted).Seconds() * 1000
			item.FirstSyncErrorMS = &value
		}
		result.GETs += pod.gets
		result.Denials += pod.denials
		require.False(tb, pod.submitted.IsZero(), "all configured Pods must be submitted")
		if !pod.succeeded.IsZero() {
			require.NotEqual(tb, "permanent", pod.group, "denied Pods must never start")
			result.Completed++
			startup := pod.succeeded.Sub(pod.submitted).Seconds() * 1000
			item.StartupMS = &startup
			switch pod.group {
			case "healthy":
				healthy = append(healthy, startup)
			case "late":
				late = append(late, startup)
			case "transient":
				require.NotZero(tb, pod.denials, "the transient fault must actually be exercised")
				require.False(tb, pod.succeeded.Before(pod.authorized))
				if !a.lateArrival.IsZero() && pod.succeeded.Before(a.lateArrival) {
					earlyTransients++
				}
				recovered := pod.succeeded.Sub(pod.authorized).Seconds() * 1000
				item.RecoveryMS = &recovered
				recovery = append(recovery, recovered)
			}
		}
		result.Pods = append(result.Pods, item)
	}
	result.Incomplete = result.Eligible - result.Completed
	require.LessOrEqual(tb, float64(result.GETs), float64(s.burst)+float64(s.qps)*elapsed.Seconds()+1,
		"volume requests must respect the shared client token bucket")
	result.HealthyP95MS = volumeRetryPercentile(healthy, s.healthy)
	result.LateHealthyP95MS = volumeRetryPercentile(late, s.late)
	result.RecoveryP95MS = volumeRetryPercentile(recovery, s.transient)
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	require.Empty(tb, limiter.unexpected, "unrecognized limiter failures invalidate the experiment")
	result.LimiterCalls = limiter.calls
	result.LimiterCanceled = limiter.canceled
	result.LimiterDeadlineRejected = limiter.deadlineRejected
	result.LimiterWaitMS = limiter.waited.Seconds() * 1000
	result.RetryAdmissionCalls = limiter.tryCalls
	result.RetryAdmissionRejected = limiter.tryRejected
	result.RetryAdmissionPolicy = "native"
	if s.headroom != nil {
		result.RetryAdmissionPolicy = s.headroom.name
	}
	result.RetryHeadroomTokens = limiter.headroomTokens
	result.RetryHeadroomRejected = limiter.headroomRejected
	result.RetryPendingRejected = limiter.pendingRejected
	if a.trace != nil {
		result.TokenTrace = a.trace.result()
	}
	if limiter.preflight != nil {
		preflight := *limiter.preflight
		preflight.Decisions = slices.Clone(preflight.Decisions)
		result.PodPreflight = &preflight
	}
	if !a.lateArrival.IsZero() {
		result.ObservedLateArrivalMS = ptr.To(a.lateArrival.Sub(limiter.started).Seconds() * 1000)
		result.TransientCompletedBeforeLateArrival = &earlyTransients
	}
	if limiter.contention != nil {
		contention := *limiter.contention
		contention.Checks = slices.Clone(contention.Checks)
		limiter.observer.mu.Lock()
		contention.Epoch = limiter.observer.epoch
		contention.Queued = slices.Clone(limiter.observer.queued)
		limiter.observer.mu.Unlock()
		result.Contention = &contention
	}
	return result
}

func runVolumeRetryScenario(tb testing.TB, s volumeRetryScenario, baseBackoff time.Duration) volumeRetryResult {
	tb.Helper()
	testKubelet := newTestKubeletWithImageList(tb, nil, false, true, true, false, false)
	tb.Cleanup(testKubelet.Cleanup)
	kubelet := testKubelet.kubelet
	pluginMgr := &volume.VolumePluginMgr{}
	require.NoError(tb, pluginMgr.InitPlugins(
		[]volume.VolumePlugin{volumeRetryFilesystemPlugin{testKubelet.volumePlugin}},
		nil, testKubelet.volumePlugin.Host))
	kubelet.volumePluginMgr = pluginMgr
	driverWatch := watch.NewRaceFreeFake()
	defer driverWatch.Stop()
	testKubelet.fakeKubeClient.AddWatchReactor("csidrivers", core.DefaultWatchReactor(driverWatch, nil))
	kubelet.podManager = orderedVolumeRetryPods{kubelet.podManager}
	kubelet.clock = clock.RealClock{}
	kubelet.crashLoopBackOff.Clock = clock.RealClock{}
	api := newVolumeRetryAPI(s, clock.RealClock{})
	server := httptest.NewServer(api)
	defer server.Close()
	// Expected denials are counted below; unexpected sync and HTTP errors fail the run.
	ctx, cancel := context.WithCancel(klog.NewContext(context.Background(), logr.Discard()))
	defer cancel()
	client, limiter, err := newVolumeRetryClient(server.URL, ctx, s.qps, s.burst, s.headroom, api.trace)
	require.NoError(tb, err)
	kubelet.kubeClient = client
	workQueue := queue.NewBasicWorkQueue(clock.RealClock{})
	workers := newPodWorkers(kubelet, kubelet.recorder, workQueue, time.Minute, baseBackoff,
		kubelet.podCache, kubelet.allocationManager).(*podWorkers)
	kubelet.podWorkers = workers
	kubelet.volumeManager = kubeletvolume.NewVolumeManager(
		false, kubelet.nodeName, kubelet.podManager, workers, client, kubelet.volumePluginMgr,
		kubelet.mounter, kubelet.hostutil, kubelet.getPodsDir(), kubelet.recorder,
		volumetest.NewBlockVolumePathHandler(), kubelet.statusManager, 0, kubelet.actuatedState)
	if limiter.preflight != nil {
		limiter.mu.Lock()
		installed := limiter.preflight.HookInstalled
		limiter.mu.Unlock()
		require.True(tb, installed, "whole-Pod preflight requires the experimental populator hook overlay")
	}
	if limiter.contention != nil {
		limiter.mu.Lock()
		installed := limiter.contention.HookInstalled
		limiter.mu.Unlock()
		require.True(tb, installed, "contention observation requires the experimental episode hook overlay")
	}

	var syncing sync.WaitGroup
	managerDone := make(chan struct{})
	go func() {
		defer close(managerDone)
		kubelet.volumeManager.Run(ctx, kubelet.sourcesReady)
	}()
	defer func() {
		cancel()
		syncing.Wait()
		<-managerDone
		limiter.close()
		testKubelet.Cleanup()
	}()
	require.NoError(tb, wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true,
		func(context.Context) (bool, error) { return kubelet.volumeManager.ReconcilerStatesHasBeenSynced(), nil }))

	type completion struct {
		pod *volumeRetryPod
		err error
	}
	results := make(chan completion, len(api.pods))
	inFlight := make(map[types.UID]bool)
	byUID := make(map[types.UID]*volumeRetryPod)
	startSync := func(pod *volumeRetryPod) {
		uid := pod.pod.UID
		require.False(tb, inFlight[uid], "a Pod must not have concurrent syncs")
		inFlight[uid] = true
		api.mu.Lock()
		pod.syncAttempts++
		attempt := pod.syncAttempts
		updateType := kubetypes.SyncPodSync
		if pod.syncAttempts == 1 {
			updateType = kubetypes.SyncPodCreate
		}
		api.mu.Unlock()
		limiter.traceSnapshot(volumeRetryTraceEvent{Kind: "sync_start", Pod: pod.pod.Name, Attempt: attempt})
		syncing.Go(func() {
			terminal, postSync, err := kubelet.SyncPod(ctx, updateType, pod.pod, nil, &kubecontainer.PodStatus{ID: uid})
			if postSync != nil {
				postSync()
			}
			if err == nil && (terminal || len(kubelet.volumeManager.GetMountedVolumesForPod(volumeutil.GetUniquePodName(pod.pod))) != s.volumes) {
				err = fmt.Errorf("Pod %s completed sync without its expected mounted volumes", pod.pod.Name)
			}
			api.mu.Lock()
			if ctx.Err() == nil {
				if err == nil {
					pod.succeeded = time.Now()
				} else {
					pod.syncErrors++
					if pod.firstSyncError.IsZero() {
						pod.firstSyncError = time.Now()
					}
					deadlineRejected := limiter.observedDeadlineRejection(err)
					if deadlineRejected {
						pod.rateDeadlineErrors++
					}
					if !strings.Contains(err.Error(), volumeRetryDenial) && !deadlineRejected {
						api.unexpected = append(api.unexpected, err.Error())
					}
				}
			}
			api.mu.Unlock()
			if ctx.Err() == nil {
				event := volumeRetryTraceEvent{Kind: "sync_success", Pod: pod.pod.Name, Attempt: attempt}
				if err != nil {
					event.Kind = "sync_error"
					event.Reason = err.Error()
				}
				limiter.traceSnapshot(event)
			}
			results <- completion{pod, err}
		})
	}
	submit := func(late bool) {
		for _, pod := range api.pods {
			if (pod.group == "late") != late {
				continue
			}
			api.mu.Lock()
			pod.submitted = time.Now()
			api.mu.Unlock()
			byUID[pod.pod.UID] = pod
			kubelet.podManager.AddPod(pod.pod)
			startSync(pod)
		}
	}

	started := time.Now()
	limiter.mu.Lock()
	limiter.started = started
	if limiter.observer != nil {
		limiter.observer.mu.Lock()
		limiter.observer.started = started
		limiter.observer.mu.Unlock()
	}
	limiter.mu.Unlock()
	if api.trace != nil {
		api.trace.mu.Lock()
		priorRequests, priorEvents := len(api.trace.Requests), len(api.trace.Events)
		api.trace.started = started
		api.trace.mu.Unlock()
		require.Zero(tb, priorRequests, "no requests may precede the experiment start")
		require.Zero(tb, priorEvents, "no trace events may precede the experiment start")
	}
	limiter.traceSnapshot(volumeRetryTraceEvent{Kind: "initial_arrival"})
	submit(false)
	lateSubmitted := s.late == 0
	window := 30 * time.Second
	if s.permanent > 0 {
		// Compare persistent-failure pressure over equal windows, even if healthy Pods finish.
		window = 20 * time.Second
	}
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	completed := 0
running:
	for {
		if s.permanent == 0 && completed == s.healthy+s.transient+s.late {
			break
		}
		select {
		case result := <-results:
			inFlight[result.pod.pod.UID] = false
			if result.err == nil {
				completed++
			} else {
				require.True(tb, strings.Contains(result.err.Error(), volumeRetryDenial) || limiter.observedDeadlineRejection(result.err),
					"unexpected sync failure: %v", result.err)
				workers.completeWork(klog.FromContext(ctx), result.pod.pod.UID, false, result.err)
			}
		case <-ticker.C:
			if !lateSubmitted && time.Since(started) >= time.Second {
				api.mu.Lock()
				api.lateArrival = time.Now()
				api.mu.Unlock()
				limiter.traceSnapshot(volumeRetryTraceEvent{Kind: "late_arrival"})
				submit(true)
				lateSubmitted = true
			}
			for _, uid := range workQueue.GetWork() {
				startSync(byUID[uid])
			}
		case <-deadline.C:
			break running
		}
	}
	elapsed := time.Since(started)
	cancel()
	syncing.Wait()
	<-managerDone
	limiter.close()
	server.Close()
	result := api.result(tb, baseBackoff, elapsed, limiter)
	if s.transient+s.permanent == 0 {
		require.Zero(tb, result.Incomplete, "the fault-free control must complete")
	}
	return result
}

func volumeRetryScenarios() []volumeRetryScenario {
	return []volumeRetryScenario{
		{name: "healthy", healthy: 100, volumes: 1, resource: "persistentvolumeclaims", latency: 5 * time.Millisecond, qps: 50, burst: 100},
		{name: "brief_pvc", healthy: 15, transient: 5, volumes: 1, resource: "persistentvolumeclaims", lag: 100 * time.Millisecond, latency: 5 * time.Millisecond, qps: 50, burst: 100},
		{name: "lagged_pv", healthy: 15, transient: 5, volumes: 1, resource: "persistentvolumes", lag: 1500 * time.Millisecond, latency: 5 * time.Millisecond, qps: 50, burst: 100},
		{name: "burst", healthy: 90, transient: 10, volumes: 1, resource: "persistentvolumes", lag: 1500 * time.Millisecond, latency: 5 * time.Millisecond, qps: 50, burst: 100},
		{name: "persistent_qps20", healthy: 4, transient: 8, permanent: 8, late: 20, volumes: 2, resource: "persistentvolumes", lag: 1500 * time.Millisecond, latency: 5 * time.Millisecond, qps: 20, burst: 40},
		{name: "slow_api", healthy: 4, transient: 8, permanent: 8, late: 20, volumes: 2, resource: "persistentvolumes", lag: 1500 * time.Millisecond, latency: 50 * time.Millisecond, qps: 50, burst: 100},
	}
}

func benchmarkVolumeRetryScenario(b *testing.B, scenario volumeRetryScenario, backoff time.Duration) {
	var gets, elapsed float64
	for range b.N {
		result := runVolumeRetryScenario(b, scenario, backoff)
		data, err := json.Marshal(result)
		require.NoError(b, err)
		b.Logf("VOLUME_RETRY_RESULT %s", data)
		gets += float64(result.GETs)
		elapsed += result.ElapsedMS
	}
	b.ReportMetric(gets/float64(b.N), "GETs/op")
	b.ReportMetric(elapsed/float64(b.N), "observation-ms/op")
}

// BenchmarkVolumeAuthorizationRetry varies only the test-configured generic Pod-sync
// backoff. It runs real HTTP, client throttling, volume management, SyncPod, and
// completeWork scheduling; a small driver replaces the outer worker/event loop.
// Runtime, mounts, status, and authorization lag are simulated. No kubeconfig is read.
// Run with GOMAXPROCS=4 and -run='^$', -bench=BenchmarkVolumeAuthorizationRetry,
// -benchtime=1x, -count=3, -timeout=30m, and -v. Filter sub-benchmarks to limit the sweep.
// JSON records report first successful sync, not container readiness or control-plane capacity.
func BenchmarkVolumeAuthorizationRetry(b *testing.B) {
	for _, scenario := range volumeRetryScenarios() {
		b.Run(scenario.name, func(b *testing.B) {
			for _, backoff := range []time.Duration{backOffPeriod, 5 * time.Second, 3 * time.Second, 2 * time.Second, time.Second, 300 * time.Millisecond, 100 * time.Millisecond} {
				b.Run("backoff="+backoff.String(), func(b *testing.B) {
					benchmarkVolumeRetryScenario(b, scenario, backoff)
				})
			}
		})
	}
}

// These policies are harness-only; none changes the production limiter API.
func BenchmarkVolumeAuthorizationHeadroom(b *testing.B) {
	benchmarkVolumeAuthorizationHeadroom(b, false)
}

// Trace and untraced runs share the same workload and admission policies.
func BenchmarkVolumeAuthorizationTokenTrace(b *testing.B) {
	benchmarkVolumeAuthorizationHeadroom(b, true)
}

func benchmarkVolumeAuthorizationHeadroom(b *testing.B, trace bool) {
	benchmarkVolumeAuthorizationPolicies(b, trace, volumeRetryScenarios(), []volumeRetryHeadroomPolicy{
		{name: "observed"},
		{name: "pending", yieldToWaiters: true},
		{name: "reserve25", percent: 25, yieldToWaiters: true},
		{name: "reserve50", percent: 50, yieldToWaiters: true},
	})
}

// These require the populator hook overlay; preflight never reserves tokens.
func BenchmarkVolumeAuthorizationPodPreflight(b *testing.B) {
	benchmarkVolumeAuthorizationPodPreflight(b, false)
}

func BenchmarkVolumeAuthorizationPodPreflightTrace(b *testing.B) {
	benchmarkVolumeAuthorizationPodPreflight(b, true)
}

func benchmarkVolumeAuthorizationPodPreflight(b *testing.B, trace bool) {
	benchmarkVolumeAuthorizationPolicies(b, trace, volumeRetryScenarios(), []volumeRetryHeadroomPolicy{
		{name: "wholepod", wholePod: true, yieldToWaiters: true},
		{name: "wholepod25", wholePod: true, percent: 25, yieldToWaiters: true},
		{name: "wholepod50", wholePod: true, percent: 50, yieldToWaiters: true},
	})
}

// The transition workload checks whether a previously uncontented episode can
// still spend capacity that a later ordinary burst needs.
func BenchmarkVolumeAuthorizationContention(b *testing.B) {
	benchmarkVolumeAuthorizationContention(b, false)
}

func BenchmarkVolumeAuthorizationContentionTrace(b *testing.B) {
	benchmarkVolumeAuthorizationContention(b, true)
}

func benchmarkVolumeAuthorizationContention(b *testing.B, trace bool) {
	scenarios := append(volumeRetryScenarios(), volumeRetryScenario{
		name: "late_pressure", healthy: 2, transient: 4, late: 20, volumes: 2,
		resource: "persistentvolumes", lag: 1500 * time.Millisecond,
		latency: 5 * time.Millisecond, qps: 20, burst: 40,
	})
	benchmarkVolumeAuthorizationPolicies(b, trace, scenarios, []volumeRetryHeadroomPolicy{
		{name: "control"},
		{name: "native25", wholePod: true, percent: 25, yieldToWaiters: true},
		{name: "observe25", wholePod: true, percent: 25, yieldToWaiters: true, observeWait: true},
		{name: "veto25", wholePod: true, percent: 25, yieldToWaiters: true, observeWait: true, contentionVeto: true},
	})
}

func benchmarkVolumeAuthorizationPolicies(b *testing.B, trace bool, scenarios []volumeRetryScenario, policies []volumeRetryHeadroomPolicy) {
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			for _, policy := range policies {
				b.Run(policy.name, func(b *testing.B) {
					scenario.headroom = &policy
					scenario.trace = trace
					benchmarkVolumeRetryScenario(b, scenario, backOffPeriod)
				})
			}
		})
	}
}

func TestVolumeRetryAPI(t *testing.T) {
	for _, deniedResource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		t.Run(deniedResource, func(t *testing.T) {
			c := clocktesting.NewFakeClock(time.Unix(1, 0))
			api := newVolumeRetryAPI(volumeRetryScenario{
				healthy: 1, transient: 1, permanent: 1, volumes: 1, resource: deniedResource, lag: time.Second,
			}, c)
			server := httptest.NewServer(api)
			defer server.Close()
			client, err := clientset.NewForConfig(&rest.Config{Host: server.URL, QPS: 50, Burst: 100})
			require.NoError(t, err)
			get := func(pod *volumeRetryPod) error {
				name := pod.pod.Name + "-volume-0"
				if deniedResource == "persistentvolumeclaims" {
					_, err := client.CoreV1().PersistentVolumeClaims(pod.pod.Namespace).Get(t.Context(), name, metav1.GetOptions{})
					return err
				}
				_, err := client.CoreV1().PersistentVolumes().Get(t.Context(), "pv-"+name, metav1.GetOptions{})
				return err
			}
			permanent, transient, healthy := api.pods[0], api.pods[1], api.pods[2]
			require.True(t, apierrors.IsForbidden(get(transient)))
			require.True(t, apierrors.IsForbidden(get(permanent)))
			require.NoError(t, get(healthy))
			c.Step(time.Second)
			require.NoError(t, get(transient))
			require.True(t, apierrors.IsForbidden(get(permanent)))
			api.mu.Lock()
			defer api.mu.Unlock()
			require.Equal(t, 1, transient.denials)
			require.Equal(t, 2, permanent.denials)
			require.Equal(t, 0, healthy.denials)
			require.Equal(t, 2, transient.gets)
			require.Equal(t, 2, permanent.gets)
			require.Equal(t, 1, healthy.gets)
			require.Empty(t, api.unexpected)
		})
	}
}

func TestVolumeRetryAPICancellation(t *testing.T) {
	api := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 1, latency: time.Minute}, clock.RealClock{})
	server := httptest.NewServer(api)
	defer server.Close()
	client, err := clientset.NewForConfig(&rest.Config{Host: server.URL, QPS: 50, Burst: 100})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.CoreV1().PersistentVolumeClaims("volume-retry-test").Get(ctx, "pod-0000-volume-0", metav1.GetOptions{})
		result <- err
	}()
	require.Eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.active == 1
	}, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled HTTP lookup did not return")
	}

	require.Eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.active == 0
	}, 5*time.Second, time.Millisecond, "cancellation must interrupt the simulated API delay")
}

func TestVolumeRetryRequestContext(t *testing.T) {
	t.Run("request metadata and cancellation", func(t *testing.T) {
		type contextKey struct{}
		key := contextKey{}
		deadline := time.Now().Add(time.Minute)
		request, cancelRequest := context.WithDeadline(context.WithValue(t.Context(), key, "request"), deadline)
		defer cancelRequest()
		lifetime := context.WithValue(t.Context(), key, "experiment")
		ctx, cancel := volumeRetryRequestContext(lifetime, request)
		defer cancel()
		actualDeadline, ok := ctx.Deadline()
		require.True(t, ok, "the limiter must see the request deadline before reserving tokens")
		require.Equal(t, deadline, actualDeadline)
		require.Equal(t, "request", ctx.Value(key))
		cancelRequest()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
	t.Run("expired request", func(t *testing.T) {
		request, cancelRequest := context.WithDeadline(t.Context(), time.Unix(1, 0))
		defer cancelRequest()
		ctx, cancel := volumeRetryRequestContext(t.Context(), request)
		defer cancel()
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	})
	t.Run("already canceled experiment", func(t *testing.T) {
		lifetime, stop := context.WithCancel(t.Context())
		stop()
		ctx, cancel := volumeRetryRequestContext(lifetime, context.Background())
		defer cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestVolumeRetryLimiterSharedAdmission(t *testing.T) {
	limiter := &volumeRetryLimiter{
		RateLimiter: flowcontrol.NewTokenBucketRateLimiter(0, 2),
		lifetime:    t.Context(),
	}
	defer limiter.close()
	require.NoError(t, limiter.Wait(t.Context()))
	require.True(t, limiter.TryAccept())
	require.False(t, limiter.TryAccept(), "normal waits and non-blocking admission must spend the same tokens")
	require.Equal(t, 1, limiter.calls)
	require.Equal(t, 2, limiter.tryCalls)
	require.Equal(t, 1, limiter.tryRejected)
	limiter.close()
	require.False(t, limiter.TryAccept(), "closed experiments must not admit more work")
	require.Equal(t, 2, limiter.tryCalls)
}

func TestVolumeRetryTokenBucketCompatibility(t *testing.T) {
	for name, newLimiter := range map[string]func(float32, int) flowcontrol.RateLimiter{
		"client-go": flowcontrol.NewTokenBucketRateLimiter,
		"observed": func(qps float32, burst int) flowcontrol.RateLimiter {
			return volumeRetryTokenBucket{rate.NewLimiter(rate.Limit(qps), burst)}
		},
		"observed wait": func(qps float32, burst int) flowcontrol.RateLimiter {
			return &volumeRetryObservedWait{Limiter: rate.NewLimiter(rate.Limit(qps), burst), started: time.Now()}
		},
	} {
		t.Run(name, func(t *testing.T) {
			limiter := newLimiter(0, 3)
			require.True(t, limiter.TryAccept())
			require.NoError(t, limiter.Wait(t.Context()))
			limiter.Accept()
			require.False(t, limiter.TryAccept(), "all three entry points must share the token supply")
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			require.ErrorContains(t, limiter.Wait(ctx), "would exceed context deadline")
			require.NoError(t, ctx.Err(), "deadline preflight must not wait for expiry")
			canceled, stop := context.WithCancel(t.Context())
			stop()
			require.ErrorIs(t, limiter.Wait(canceled), context.Canceled)

			refilling := newLimiter(100, 1)
			require.True(t, refilling.TryAccept())
			refillCtx, cancelRefill := context.WithTimeout(t.Context(), time.Second)
			defer cancelRefill()
			require.NoError(t, refilling.Wait(refillCtx), "normal waits must still receive refilled tokens")
		})
	}
}

func TestVolumeRetryObservedWait(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprintf("native_engine=%t", !observed), func(t *testing.T) {
			bucket := rate.NewLimiter(1, 1)
			var limiter flowcontrol.RateLimiter = volumeRetryTokenBucket{bucket}
			var observer *volumeRetryObservedWait
			if observed {
				observer = &volumeRetryObservedWait{Limiter: bucket, started: time.Now()}
				limiter = observer
			}
			require.NoError(t, limiter.Wait(t.Context()))
			if observer != nil {
				require.Zero(t, observer.contentionEpoch(), "an immediate request must not count clock or lock overhead as queuing")
			}
			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				result <- limiter.Wait(ctx)
			}()
			defer func() {
				cancel()
				<-done
			}()
			require.Eventually(t, func() bool { return bucket.Tokens() < -0.5 }, time.Second, time.Millisecond,
				"the ordinary wait must reserve a future token")
			deadline, stop := context.WithTimeout(t.Context(), 1500*time.Millisecond)
			defer stop()
			require.ErrorContains(t, limiter.Wait(deadline), volumeRetryDeadlineRejection,
				"a second future token cannot fit the caller's deadline")
			require.NoError(t, deadline.Err())
			require.Greater(t, bucket.Tokens(), -1.0, "deadline rejection must not leave another reservation behind")
			if observer != nil {
				require.EqualValues(t, 1, observer.contentionEpoch(), "preflight rejection must not count as another queued request")
			}
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
			require.GreaterOrEqual(t, bucket.Tokens(), 0.0, "canceling the queued request must restore its reservation")
			if observer != nil {
				require.EqualValues(t, 1, observer.contentionEpoch(), "a canceled wait still establishes that contention occurred")
			}
		})
	}
	t.Run("zero refill deadline rejection is observational", func(t *testing.T) {
		bucket := rate.NewLimiter(0, 1)
		limiter := &volumeRetryObservedWait{Limiter: bucket, started: time.Now()}
		require.True(t, limiter.TryAccept())
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		for range 3 {
			require.ErrorContains(t, limiter.Wait(ctx), volumeRetryDeadlineRejection)
		}
		require.Zero(t, bucket.Tokens())
		require.Zero(t, limiter.contentionEpoch())
	})
	t.Run("burst and unlimited behavior match the native engine", func(t *testing.T) {
		for _, limit := range []rate.Limit{1, rate.Inf} {
			native := volumeRetryTokenBucket{rate.NewLimiter(limit, 0)}
			observed := &volumeRetryObservedWait{Limiter: rate.NewLimiter(limit, 0), started: time.Now()}
			actual, expected := observed.Wait(t.Context()), native.Wait(t.Context())
			if expected == nil {
				require.NoError(t, actual)
			} else {
				require.EqualError(t, actual, expected.Error())
			}
			require.Zero(t, observed.contentionEpoch())
		}
	})
}

func TestVolumeRetryHeadroom(t *testing.T) {
	bucket := rate.NewLimiter(0, 4)
	limiter := &volumeRetryLimiter{
		RateLimiter: volumeRetryTokenBucket{bucket}, lifetime: t.Context(),
		tokenBucket: bucket, headroomTokens: 2, yieldToWaiters: true,
	}
	defer limiter.close()
	require.NoError(t, limiter.Wait(t.Context()))
	require.True(t, limiter.TryAccept(), "a retry may use the token above the reserve")
	require.False(t, limiter.TryAccept(), "a retry must not consume reserved ordinary capacity")
	require.Equal(t, 2.0, bucket.Tokens())
	require.NoError(t, limiter.Wait(t.Context()))
	require.NoError(t, limiter.Wait(t.Context()), "ordinary requests may consume the entire reserve")
	require.Zero(t, bucket.Tokens())
	require.Equal(t, 1, limiter.headroomRejected)
}

func TestVolumeRetryContentionVeto(t *testing.T) {
	for _, veto := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", veto), func(t *testing.T) {
			observer := &volumeRetryObservedWait{Limiter: rate.NewLimiter(50, 3), started: time.Now()}
			limiter := &volumeRetryLimiter{
				RateLimiter: observer, observer: observer, lifetime: t.Context(),
				tokenBucket: observer.Limiter, yieldToWaiters: true, started: observer.started,
				contention: &volumeRetryContentionResult{VetoEnabled: veto},
			}
			client := volumeRetryContentionClient{volumeRetryPodPreflightClient{limiter: limiter}}
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", UID: "pod"}}
			oldEpoch := client.InitialVolumeRetryContentionEpoch()
			for range 3 {
				require.NoError(t, limiter.Wait(t.Context()))
			}
			ctx, cancel := context.WithCancel(t.Context())
			result, done := make(chan error, 1), make(chan struct{})
			go func() {
				defer close(done)
				result <- limiter.Wait(ctx)
			}()
			defer func() {
				cancel()
				<-done
				limiter.close()
			}()
			require.Eventually(t, func() bool { return observer.contentionEpoch() > oldEpoch }, time.Second, time.Millisecond)
			cancel()
			if err := <-result; err != nil {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Eventually(t, func() bool { return observer.Tokens() == 3 }, time.Second, time.Millisecond,
				"refill must restore capacity without erasing the contention signal")
			require.Equal(t, !veto, client.AdmitInitialVolumeRetryAfterContention(t.Context(), pod, oldEpoch))
			require.Equal(t, !veto, limiter.tryAccept(&volumeRetryContentionEpisode{pod.UID, oldEpoch}))
			if veto {
				require.Equal(t, 3.0, observer.Tokens(), "a stale episode must spend nothing even with a full bucket")
			}
			freshEpoch := client.InitialVolumeRetryContentionEpoch()
			require.Greater(t, freshEpoch, oldEpoch)
			fresh := pod.DeepCopy()
			fresh.UID = "new-episode"
			require.True(t, client.AdmitInitialVolumeRetryAfterContention(t.Context(), fresh, freshEpoch))
			require.True(t, limiter.tryAccept(&volumeRetryContentionEpisode{fresh.UID, freshEpoch}),
				"historical contention must not globally disable a fresh episode")
			require.Empty(t, limiter.unexpected)
		})
	}
}

func TestVolumeRetryWholePodPreflight(t *testing.T) {
	newLimiter := func(ctx context.Context, capacity, reserve int) *volumeRetryLimiter {
		bucket := rate.NewLimiter(0, capacity)
		return &volumeRetryLimiter{
			RateLimiter: volumeRetryTokenBucket{bucket}, lifetime: ctx,
			tokenBucket: bucket, headroomTokens: reserve, yieldToWaiters: true,
			started: time.Now(), preflight: &volumeRetryPodPreflightResult{},
		}
	}
	for _, tc := range []struct {
		name      string
		unused    bool
		initMount bool
		cost      int
	}{
		{name: "two mounted PVCs include verification", cost: 6},
		{name: "unused PVC does not consume the estimate", unused: true, cost: 3},
		{name: "init-container PVC remains part of startup", initMount: true, cost: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 2}, clock.RealClock{})
			pod := api.pods[0].pod
			if tc.initMount {
				pod.Spec.InitContainers = []v1.Container{{Name: "init", VolumeMounts: []v1.VolumeMount{pod.Spec.Containers[0].VolumeMounts[1]}}}
			}
			if tc.unused || tc.initMount {
				pod.Spec.Containers[0].VolumeMounts = pod.Spec.Containers[0].VolumeMounts[:1]
			}
			const reserve = 2
			required := tc.cost + reserve
			limiter := newLimiter(t.Context(), required, reserve)
			defer limiter.close()
			require.True(t, limiter.admitPodRetry(t.Context(), pod), "exactly enough estimated capacity must admit the pass")
			require.Equal(t, float64(required), limiter.tokenBucket.Tokens(), "preflight must not reserve or spend tokens")
			require.NoError(t, limiter.Wait(t.Context()))
			require.False(t, limiter.admitPodRetry(t.Context(), pod), "one token below the whole-Pod threshold must refuse")
			require.Equal(t, float64(required-1), limiter.tokenBucket.Tokens(), "refusal must not spend tokens")
			require.Equal(t, tc.cost, limiter.preflight.Decisions[0].CostTokens)
			require.Equal(t, "capacity", limiter.preflight.Decisions[1].Reason)
			for range required - 1 {
				require.NoError(t, limiter.Wait(t.Context()), "ordinary work must retain access to every token")
			}
			require.False(t, limiter.TryAccept(), "an earlier preflight must not grant credits after ordinary work spends the capacity")
			require.Equal(t, 1, limiter.preflight.Admitted)
			require.Equal(t, 1, limiter.preflight.Rejected)
			require.Empty(t, limiter.unexpected)
		})
	}
	t.Run("pending ordinary wait wins before its reservation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		limiter := newLimiter(ctx, 6, 0)
		gate := volumeRetryWaitGate{limiter.RateLimiter, make(chan struct{}), make(chan struct{})}
		limiter.RateLimiter = gate
		result, done := make(chan error, 1), make(chan struct{})
		go func() {
			defer close(done)
			result <- limiter.Wait(ctx)
		}()
		defer func() {
			cancel()
			<-done
			limiter.close()
		}()
		select {
		case <-gate.entered:
		case err := <-result:
			t.Fatalf("ordinary wait did not reach its admission barrier: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		pod := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 1}, clock.RealClock{}).pods[0].pod
		require.False(t, limiter.admitPodRetry(ctx, pod))
		require.Equal(t, "pending", limiter.preflight.Decisions[0].Reason)
		require.Equal(t, 6.0, limiter.tokenBucket.Tokens())
		close(gate.release)
		require.NoError(t, <-result)
		require.True(t, limiter.admitPodRetry(ctx, pod))
		require.Equal(t, 5.0, limiter.tokenBucket.Tokens(), "only the ordinary request may have spent a token")
	})
	t.Run("unsupported work invalidates the experiment", func(t *testing.T) {
		limiter := newLimiter(t.Context(), 6, 0)
		defer limiter.close()
		pod := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 1}, clock.RealClock{}).pods[0].pod
		pod.Spec.Volumes[0].VolumeSource = v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}
		require.False(t, limiter.admitPodRetry(t.Context(), pod))
		require.Len(t, limiter.unexpected, 1)
		require.Contains(t, limiter.unexpected[0], "cost model supports only filesystem PVCs")
		require.Equal(t, "unsupported", limiter.preflight.Decisions[0].Reason)
		require.Equal(t, 6.0, limiter.tokenBucket.Tokens())
	})
	t.Run("canceled request spends no tokens", func(t *testing.T) {
		limiter := newLimiter(t.Context(), 6, 0)
		defer limiter.close()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		pod := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 1}, clock.RealClock{}).pods[0].pod
		require.False(t, limiter.admitPodRetry(ctx, pod))
		require.Equal(t, "canceled", limiter.preflight.Decisions[0].Reason)
		require.Equal(t, 6.0, limiter.tokenBucket.Tokens())
		require.Empty(t, limiter.unexpected)
	})
}

type volumeRetryWaitGate struct {
	flowcontrol.RateLimiter
	entered chan struct{}
	release chan struct{}
}

func (g volumeRetryWaitGate) Wait(ctx context.Context) error {
	close(g.entered)
	select {
	case <-g.release:
		return g.RateLimiter.Wait(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestVolumeRetryHeadroomYieldsToPendingWait(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	bucket := rate.NewLimiter(0, 4)
	gate := volumeRetryWaitGate{volumeRetryTokenBucket{bucket}, make(chan struct{}), make(chan struct{})}
	limiter := &volumeRetryLimiter{
		RateLimiter: gate, lifetime: ctx, tokenBucket: bucket, yieldToWaiters: true,
	}
	result, done := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(done)
		result <- limiter.Wait(ctx)
	}()
	defer func() {
		cancel()
		<-done
		limiter.close()
	}()
	select {
	case <-gate.entered:
	case err := <-result:
		t.Fatalf("ordinary wait did not reach its admission barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, limiter.TryAccept(), "retry admission must not race an ordinary token reservation")
	require.Equal(t, 4.0, bucket.Tokens())
	require.Equal(t, 1, limiter.pendingRejected)
	close(gate.release)
	require.NoError(t, <-result)
}

// Exercise request-local throttle replacement without importing private populator helpers.
type volumeRetryAdmissionProbe struct{ flowcontrol.RateLimiter }

func (l volumeRetryAdmissionProbe) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !l.TryAccept() {
		return fmt.Errorf("test non-blocking admission refused")
	}
	return nil
}

func TestVolumeRetryTokenTrace(t *testing.T) {
	newClient := func(t *testing.T, pods, burst int) (*volumeRetryAPI, clientset.Interface, *volumeRetryLimiter) {
		api := newVolumeRetryAPI(volumeRetryScenario{healthy: pods, volumes: 1, trace: true}, clock.RealClock{})
		server := httptest.NewServer(api)
		t.Cleanup(server.Close)
		client, limiter, err := newVolumeRetryClient(server.URL, t.Context(), 0, burst,
			&volumeRetryHeadroomPolicy{name: "observed"}, api.trace)
		require.NoError(t, err)
		t.Cleanup(limiter.close)
		return api, client, limiter
	}
	t.Run("request-local replacement and ordinary deadline", func(t *testing.T) {
		api, client, limiter := newClient(t, 1, 2)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := client.CoreV1().PersistentVolumeClaims("volume-retry-test").Get(ctx, "pod-0000-volume-0", metav1.GetOptions{})
		require.NoError(t, err)
		retry := func() error {
			return client.CoreV1().RESTClient().Get().
				Namespace("volume-retry-test").Resource("persistentvolumeclaims").Name("pod-0000-volume-0").
				Throttle(volumeRetryAdmissionProbe{limiter}).Do(ctx).Error()
		}
		require.NoError(t, retry())
		require.ErrorContains(t, retry(), "test non-blocking admission refused")
		require.Zero(t, limiter.tokenBucket.Tokens(), "tracing must not double-charge or bypass shared tokens")
		require.Equal(t, 1, limiter.calls)
		require.Equal(t, 2, limiter.tryCalls)
		require.Equal(t, 1, limiter.tryRejected)
		trace := api.trace.result()
		require.Len(t, trace.Requests, 3)
		for i, request := range trace.Requests {
			require.Equal(t, i+1, request.ID)
			require.NotEmpty(t, request.Callers)
			require.Equal(t, []int{1, 0, 0}[i], request.OrdinaryWaitCalls)
			require.Equal(t, []int{1, 1, 0}[i], request.APIGETs)
		}
		require.Equal(t, "pod-0000", trace.Requests[0].Pod)
		require.Equal(t, trace.Requests[0].Path, trace.Requests[1].Path)
		var admissions []volumeRetryTraceEvent
		for _, event := range trace.Events {
			if event.Kind == "retry_admission" {
				admissions = append(admissions, event)
			}
		}
		require.Len(t, admissions, 2)
		require.True(t, *admissions[0].Accepted)
		require.Equal(t, 1.0, *admissions[0].TokensBefore)
		require.Zero(t, *admissions[0].Tokens)
		require.False(t, *admissions[1].Accepted)
		require.Equal(t, "tokens", admissions[1].Reason)
	})
	t.Run("concurrent ordinary requests retain identity", func(t *testing.T) {
		api, client, _ := newClient(t, 2, 2)
		var requests sync.WaitGroup
		errors := make(chan error, 2)
		for i := range 2 {
			requests.Go(func() {
				_, err := client.CoreV1().PersistentVolumeClaims("volume-retry-test").Get(
					t.Context(), fmt.Sprintf("pod-%04d-volume-0", i), metav1.GetOptions{})
				errors <- err
			})
		}
		requests.Wait()
		require.NoError(t, <-errors)
		require.NoError(t, <-errors)
		trace := api.trace.result()
		require.Len(t, trace.Requests, 2)
		var pods []string
		for _, request := range trace.Requests {
			require.Equal(t, 1, request.OrdinaryWaitCalls)
			require.Equal(t, 1, request.APIGETs)
			require.Contains(t, request.Path, request.Pod+"-volume-0")
			pods = append(pods, request.Pod)
		}
		require.ElementsMatch(t, []string{"pod-0000", "pod-0001"}, pods)
		for _, event := range trace.Events {
			require.Contains(t, []int{1, 2}, event.RequestID)
		}
	})
	t.Run("unissued requests preserve deadline and cancellation", func(t *testing.T) {
		api, client, limiter := newClient(t, 1, 1)
		get := func(ctx context.Context) error {
			_, err := client.CoreV1().PersistentVolumeClaims("volume-retry-test").Get(ctx, "pod-0000-volume-0", metav1.GetOptions{})
			return err
		}
		require.NoError(t, get(t.Context()))
		deadline, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorContains(t, get(deadline), volumeRetryDeadlineRejection)
		require.NoError(t, deadline.Err(), "tracing must not convert preflight refusal into deadline expiry")
		canceled, stop := context.WithCancel(t.Context())
		stop()
		require.ErrorIs(t, get(canceled), context.Canceled)
		require.Zero(t, limiter.tokenBucket.Tokens())
		require.Equal(t, 1, limiter.deadlineRejected)
		trace := api.trace.result()
		require.Len(t, trace.Requests, 3)
		require.Equal(t, 1, trace.Requests[0].APIGETs)
		require.Zero(t, trace.Requests[1].APIGETs)
		require.Zero(t, trace.Requests[2].APIGETs)
		require.Equal(t, 1, trace.Requests[1].OrdinaryWaitCalls)
	})
}

func TestVolumeRetryLimiterDeadlinePreflight(t *testing.T) {
	limiter := &volumeRetryLimiter{
		RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1.0/60, 1),
		lifetime:    t.Context(),
	}
	defer limiter.close()
	require.NoError(t, limiter.Wait(t.Context()))
	request, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := limiter.Wait(request)
	require.ErrorContains(t, err, "would exceed context deadline")
	require.NoError(t, request.Err(), "preflight rejection must not wait for request cancellation")
	require.Equal(t, 1, limiter.deadlineRejected)
	require.Zero(t, limiter.canceled, "deadline preflight is not a canceled wait")
	require.Empty(t, limiter.unexpected)
	require.True(t, limiter.observedDeadlineRejection(fmt.Errorf("client rate limiter Wait returned an error: %w", err)))
	require.False(t, limiter.observedDeadlineRejection(fmt.Errorf("unrelated sync failure")))
}

func TestVolumeRetryExperimentCancellation(t *testing.T) {
	t.Run("limiter", func(t *testing.T) {
		lifetime, cancel := context.WithCancel(t.Context())
		defer cancel()
		limiter := &volumeRetryLimiter{
			RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1),
			lifetime:    lifetime,
		}
		require.NoError(t, limiter.Wait(context.Background()))
		result := make(chan error, 1)
		go func() { result <- limiter.Wait(context.Background()) }()
		require.Eventually(t, func() bool {
			limiter.mu.Lock()
			defer limiter.mu.Unlock()
			return limiter.pending == 1
		}, 5*time.Second, time.Millisecond)
		cancel()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("experiment cancellation did not interrupt the limiter")
		}
		limiter.close()
		require.ErrorIs(t, limiter.Wait(context.Background()), context.Canceled)
	})
	for _, trace := range []bool{false, true} {
		t.Run(fmt.Sprintf("HTTP/trace=%t", trace), func(t *testing.T) {
			api := newVolumeRetryAPI(volumeRetryScenario{healthy: 1, volumes: 1, latency: time.Minute, trace: trace}, clock.RealClock{})
			server := httptest.NewServer(api)
			defer server.Close()
			lifetime, cancel := context.WithCancel(t.Context())
			defer cancel()
			var policy *volumeRetryHeadroomPolicy
			if trace {
				policy = &volumeRetryHeadroomPolicy{name: "observed"}
			}
			client, limiter, err := newVolumeRetryClient(server.URL, lifetime, 50, 100, policy, api.trace)
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() {
				_, err := client.CoreV1().PersistentVolumeClaims("volume-retry-test").Get(context.Background(), "pod-0000-volume-0", metav1.GetOptions{})
				result <- err
			}()
			require.Eventually(t, func() bool {
				api.mu.Lock()
				defer api.mu.Unlock()
				return api.active == 1
			}, 5*time.Second, time.Millisecond)
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("experiment cancellation did not interrupt a context.Background lookup")
			}
			limiter.close()
			require.Eventually(t, func() bool {
				api.mu.Lock()
				defer api.mu.Unlock()
				return api.active == 0
			}, 5*time.Second, time.Millisecond)
			if trace {
				var canceled []volumeRetryTraceEvent
				for _, event := range api.trace.result().Events {
					if event.Kind == "api_canceled" {
						canceled = append(canceled, event)
					}
				}
				require.Len(t, canceled, 1, "an interrupted API GET must not look like an unfinished trace")
				require.Equal(t, 1, canceled[0].RequestID)
				require.Equal(t, context.Canceled.Error(), canceled[0].Reason)
			}
		})
	}
}
