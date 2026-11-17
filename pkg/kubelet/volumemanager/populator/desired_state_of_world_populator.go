/*
Copyright 2016 The Kubernetes Authors.

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

/*
Package populator implements interfaces that monitor and keep the states of the
caches in sync with the "ground truth".
*/
package populator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/utils/clock"
)

const (
	initialVolumeRetryTimeout   = 3 * time.Second
	initialVolumeRetryMaxPasses = 4
	initialVolumeRetryDelay     = 100 * time.Millisecond
	initialVolumeRetryJitter    = 0.1
)

// DesiredStateOfWorldPopulator periodically loops through the list of active
// pods and ensures that each one exists in the desired state of the world cache
// if it has volumes. It also verifies that the pods in the desired state of the
// world cache still exist, if not, it removes them.
type DesiredStateOfWorldPopulator interface {
	Run(ctx context.Context, sourcesReady config.SourcesReady)

	// ReprocessPod sets value for the specified pod in processedPods
	// to false, forcing it to be reprocessed. This is required to enable
	// remounting volumes on pod updates (volumes like Downward API volumes
	// depend on this behavior to ensure volume content is updated).
	ReprocessPod(podName volumetypes.UniquePodName)

	// HasAddedPods returns whether the populator has looped through the list
	// of active pods and added them to the desired state of the world cache,
	// at a time after sources are all ready, at least once. It does not
	// return true before sources are all ready because before then, there is
	// a chance many or all pods are missing from the list of active pods and
	// so few to none will have been added.
	HasAddedPods() bool
}

// PodStateProvider can determine if a pod is going to be terminated.
type PodStateProvider interface {
	ShouldPodContainersBeTerminating(types.UID) bool
	ShouldPodRuntimeBeRemoved(types.UID) bool
}

// PodManager is the subset of methods the manager needs to observe the actual state of the kubelet.
// See pkg/k8s.io/kubernetes/pkg/kubelet/pod.Manager for method godoc.
type PodManager interface {
	GetPodByUID(types.UID) (*v1.Pod, bool)
	GetPods() []*v1.Pod
}

// NewDesiredStateOfWorldPopulator returns a new instance of
// DesiredStateOfWorldPopulator.
//
// kubeClient - used to fetch PV and PVC objects from the API server
// loopSleepDuration - the amount of time the populator loop sleeps between
// successive executions
//
// podManager - the kubelet podManager that is the source of truth for the pods
// that exist on this host
//
// desiredStateOfWorld - the cache to populate
// clock - controls the initial volume-authorization retry budget
func NewDesiredStateOfWorldPopulator(
	kubeClient clientset.Interface,
	loopSleepDuration time.Duration,
	podManager PodManager,
	podStateProvider PodStateProvider,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	csiMigratedPluginManager csimigration.PluginManager,
	intreeToCSITranslator csimigration.InTreeToCSITranslator,
	volumePluginMgr *volume.VolumePluginMgr,
	clock clock.PassiveClock) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		kubeClient:          kubeClient,
		loopSleepDuration:   loopSleepDuration,
		podManager:          podManager,
		podStateProvider:    podStateProvider,
		desiredStateOfWorld: desiredStateOfWorld,
		actualStateOfWorld:  actualStateOfWorld,
		pods: processedPods{
			processedPods:        make(map[volumetypes.UniquePodName]bool),
			initialVolumeRetries: make(map[volumetypes.UniquePodName]initialVolumeRetryState)},
		hasAddedPods:             false,
		hasAddedPodsLock:         sync.RWMutex{},
		csiMigratedPluginManager: csiMigratedPluginManager,
		intreeToCSITranslator:    intreeToCSITranslator,
		volumePluginMgr:          volumePluginMgr,
		clock:                    clock,
	}
}

type desiredStateOfWorldPopulator struct {
	kubeClient               clientset.Interface
	loopSleepDuration        time.Duration
	podManager               PodManager
	podStateProvider         PodStateProvider
	desiredStateOfWorld      cache.DesiredStateOfWorld
	actualStateOfWorld       cache.ActualStateOfWorld
	pods                     processedPods
	hasAddedPods             bool
	hasAddedPodsLock         sync.RWMutex
	csiMigratedPluginManager csimigration.PluginManager
	intreeToCSITranslator    csimigration.InTreeToCSITranslator
	volumePluginMgr          *volume.VolumePluginMgr
	clock                    clock.PassiveClock
}

type processedPods struct {
	processedPods        map[volumetypes.UniquePodName]bool
	initialVolumeRetries map[volumetypes.UniquePodName]initialVolumeRetryState
	sync.RWMutex
}

type initialVolumeRetryState struct {
	finished     bool
	firstFailure time.Time
	deadline     time.Time
	nextRetry    time.Time
	passes       int
	errors       []string
}

var errInitialVolumeRetryCapacity = errors.New("shared client capacity unavailable for initial volume retry")

type initialVolumeRetryContextKey struct{}

type initialVolumeRetryRateLimiter struct {
	flowcontrol.RateLimiter
}

func (l initialVolumeRetryRateLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !l.TryAccept() {
		return errInitialVolumeRetryCapacity
	}
	return nil
}

type initialVolumeRetryRESTClient struct {
	rest.Interface
}

func (c initialVolumeRetryRESTClient) Get() *rest.Request {
	// Replace this request's wait, not the shared limiter. Successful admission
	// consumes exactly one shared token without reserving future capacity.
	return c.Interface.Get().Throttle(initialVolumeRetryRateLimiter{c.GetRateLimiter()})
}

func (dswp *desiredStateOfWorldPopulator) volumeClient(ctx context.Context) corev1client.CoreV1Interface {
	client := dswp.kubeClient.CoreV1()
	if ctx.Value(initialVolumeRetryContextKey{}) == nil {
		return client
	}
	restClient := client.RESTClient()
	if restClient == nil || restClient.GetRateLimiter() == nil {
		// Clients without a shared REST limiter retain their configured behavior.
		return client
	}
	return corev1client.New(initialVolumeRetryRESTClient{restClient})
}

func (s initialVolumeRetryState) exhausted(now time.Time) bool {
	return !now.Before(s.deadline) || s.passes >= initialVolumeRetryMaxPasses
}

func (dswp *desiredStateOfWorldPopulator) Run(ctx context.Context, sourcesReady config.SourcesReady) {
	// Wait for the completion of a loop that started after sources are all ready, then set hasAddedPods accordingly
	logger := klog.FromContext(ctx)
	logger.Info("Desired state populator starts to run")
	_ = wait.PollUntilContextCancel(ctx, dswp.loopSleepDuration, false, func(ctx context.Context) (bool, error) {
		done := sourcesReady.AllReady()
		dswp.populatorLoop(ctx)
		return done, nil
	})
	dswp.hasAddedPodsLock.Lock()
	if !dswp.hasAddedPods {
		logger.Info("Finished populating initial desired state of world")
		dswp.hasAddedPods = true
	}
	dswp.hasAddedPodsLock.Unlock()
	wait.UntilWithContext(ctx, dswp.populatorLoop, dswp.loopSleepDuration)
}

func (dswp *desiredStateOfWorldPopulator) ReprocessPod(
	podName volumetypes.UniquePodName) {
	dswp.markPodProcessingFailed(podName)
}

func (dswp *desiredStateOfWorldPopulator) HasAddedPods() bool {
	dswp.hasAddedPodsLock.RLock()
	defer dswp.hasAddedPodsLock.RUnlock()
	return dswp.hasAddedPods
}

func (dswp *desiredStateOfWorldPopulator) populatorLoop(ctx context.Context) {
	logger := klog.FromContext(ctx)
	dswp.findAndAddNewPods(ctx)
	dswp.findAndRemoveDeletedPods(logger)
}

// Iterate through all pods and add to desired state of world if they don't
// exist but should
func (dswp *desiredStateOfWorldPopulator) findAndAddNewPods(ctx context.Context) {
	var retryPods []*v1.Pod
	for _, pod := range dswp.podManager.GetPods() {
		if !dswp.shouldProcessPodVolumes(ctx, pod) {
			continue
		}
		podName := util.GetUniquePodName(pod)
		dswp.pods.RLock()
		if dswp.pods.processedPods[podName] {
			dswp.pods.RUnlock()
			continue
		}
		state, retrying := dswp.pods.initialVolumeRetries[podName]
		dswp.pods.RUnlock()
		if retrying && !state.finished && !state.exhausted(dswp.clock.Now()) &&
			!dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
			// Fast retries share the API client with first-time lookups. Finish
			// ordinary work in this snapshot before spending more retry capacity.
			retryPods = append(retryPods, pod)
			continue
		}
		dswp.processPodVolumes(ctx, pod)
	}

	var nextPod *v1.Pod
	var nextRetry time.Time
	for _, pod := range retryPods {
		if ctx.Err() != nil {
			return
		}
		// Ordinary lookups may block long enough for this Pod to change or disappear.
		currentPod, exists := dswp.podManager.GetPodByUID(pod.UID)
		if !exists || !dswp.shouldProcessPodVolumes(ctx, currentPod) {
			continue
		}
		if dswp.podStateProvider.ShouldPodContainersBeTerminating(currentPod.UID) {
			// Reconstruction remains ordinary work if termination began while
			// fresh lookups were running.
			dswp.processPodVolumes(ctx, currentPod)
			continue
		}
		podName := util.GetUniquePodName(currentPod)
		dswp.pods.RLock()
		state, retrying := dswp.pods.initialVolumeRetries[podName]
		dswp.pods.RUnlock()
		if !retrying || state.finished {
			continue
		}
		now := dswp.clock.Now()
		if state.exhausted(now) {
			dswp.failInitialVolumeRetry(ctx, podName, "retry budget exhausted", state.firstFailure)
			continue
		}
		if now.Before(state.nextRetry) {
			continue
		}
		if nextPod == nil || state.nextRetry.Before(nextRetry) ||
			(state.nextRetry.Equal(nextRetry) && currentPod.UID < nextPod.UID) {
			nextPod, nextRetry = currentPod, state.nextRetry
		}
	}
	if nextPod != nil && ctx.Err() == nil {
		// Admit a new Pod snapshot before starting another optional retry pass.
		currentPod, exists := dswp.podManager.GetPodByUID(nextPod.UID)
		if exists && dswp.shouldProcessPodVolumes(ctx, currentPod) {
			dswp.processPodVolumes(ctx, currentPod)
		}
	}
}

func (dswp *desiredStateOfWorldPopulator) shouldProcessPodVolumes(ctx context.Context, pod *v1.Pod) bool {
	// Keep consistency of adding pod during reconstruction.
	if dswp.hasAddedPods && dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
		// Do not (re)add volumes for pods that can't also be starting containers.
		dswp.failInitialVolumeRetry(ctx, util.GetUniquePodName(pod), "pod is terminating", time.Time{})
		return false
	}
	if !dswp.hasAddedPods && dswp.podStateProvider.ShouldPodRuntimeBeRemoved(pod.UID) {
		// Reconstruction is needed only if containers may still be running.
		dswp.failInitialVolumeRetry(ctx, util.GetUniquePodName(pod), "pod runtime can be removed", time.Time{})
		return false
	}
	return true
}

// Iterate through all pods in desired state of world, and remove if they no
// longer exist
func (dswp *desiredStateOfWorldPopulator) findAndRemoveDeletedPods(logger klog.Logger) {
	podsFromCache := make(map[volumetypes.UniquePodName]struct{})
	for _, volumeToMount := range dswp.desiredStateOfWorld.GetVolumesToMount() {
		podsFromCache[volumetypes.UniquePodName(volumeToMount.Pod.UID)] = struct{}{}
		pod, podExists := dswp.podManager.GetPodByUID(volumeToMount.Pod.UID)
		if podExists {

			// check if the attachability has changed for this volume
			if volumeToMount.PluginIsAttachable {
				attachableVolumePlugin, err := dswp.volumePluginMgr.FindAttachablePluginBySpec(volumeToMount.VolumeSpec)
				// only this means the plugin is truly non-attachable
				if err == nil && attachableVolumePlugin == nil {
					// It is not possible right now for a CSI plugin to be both attachable and non-deviceMountable
					// So the uniqueVolumeName should remain the same after the attachability change
					dswp.desiredStateOfWorld.MarkVolumeAttachability(volumeToMount.VolumeName, false)
					dswp.actualStateOfWorld.MarkVolumeAttachability(volumeToMount.VolumeName, false)
					logger.Info("Volume changes from attachable to non-attachable", "volumeName", volumeToMount.VolumeName)
					continue
				}
			}

			// Exclude known pods that we expect to be running
			if !dswp.podStateProvider.ShouldPodRuntimeBeRemoved(pod.UID) {
				continue
			}
		}

		// Once a pod has been deleted from kubelet pod manager, do not delete
		// it immediately from volume manager. Instead, check the kubelet
		// pod state provider to verify that all containers in the pod have been
		// terminated.
		if !dswp.podStateProvider.ShouldPodRuntimeBeRemoved(volumeToMount.Pod.UID) {
			logger.V(4).Info("Pod still has one or more containers in the non-exited state and will not be removed from desired state", "pod", klog.KObj(volumeToMount.Pod))
			continue
		}
		var volumeToMountSpecName string
		if volumeToMount.VolumeSpec != nil {
			volumeToMountSpecName = volumeToMount.VolumeSpec.Name()
		}
		removed := dswp.actualStateOfWorld.PodRemovedFromVolume(volumeToMount.PodName, volumeToMount.VolumeName)
		if removed && podExists {
			logger.V(4).Info("Actual state does not yet have volume mount information and pod still exists in pod manager, skip removing volume from desired state", "pod", klog.KObj(volumeToMount.Pod), "podUID", volumeToMount.Pod.UID, "volumeName", volumeToMountSpecName)
			continue
		}
		logger.V(4).Info("Removing volume from desired state", "pod", klog.KObj(volumeToMount.Pod), "podUID", volumeToMount.Pod.UID, "volumeName", volumeToMountSpecName)
		dswp.desiredStateOfWorld.DeletePodFromVolume(
			volumeToMount.PodName, volumeToMount.VolumeName)
		dswp.deleteProcessedPod(volumeToMount.PodName)
	}

	// Cleanup orphanded entries from processedPods
	dswp.pods.Lock()
	orphanedPods := make([]volumetypes.UniquePodName, 0, len(dswp.pods.processedPods))
	for k := range dswp.pods.processedPods {
		if _, ok := podsFromCache[k]; !ok {
			orphanedPods = append(orphanedPods, k)
		}
	}
	dswp.pods.Unlock()
	for _, orphanedPod := range orphanedPods {
		uid := types.UID(orphanedPod)
		_, podExists := dswp.podManager.GetPodByUID(uid)
		if !podExists && dswp.podStateProvider.ShouldPodRuntimeBeRemoved(uid) {
			dswp.deleteProcessedPod(orphanedPod)
		}
	}

	podsWithError := dswp.desiredStateOfWorld.GetPodsWithErrors()
	for _, podName := range podsWithError {
		if _, podExists := dswp.podManager.GetPodByUID(types.UID(podName)); !podExists {
			dswp.desiredStateOfWorld.PopPodErrors(podName)
		}
	}
}

// processPodVolumes processes the volumes in the given pod and adds them to the
// desired state of the world.
func (dswp *desiredStateOfWorldPopulator) processPodVolumes(ctx context.Context, pod *v1.Pod) {
	if pod == nil {
		return
	}

	logger := klog.FromContext(ctx)
	uniquePodName := util.GetUniquePodName(pod)
	if dswp.podPreviouslyProcessed(uniquePodName) {
		return
	}

	allowInitialRetry := !dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID)
	if !allowInitialRetry {
		dswp.failInitialVolumeRetry(ctx, uniquePodName, "pod is terminating", time.Time{})
	}
	retryCtx, cancel, process := dswp.initialVolumeRetryContext(ctx, uniquePodName)
	if !process {
		return
	}
	if cancel != nil {
		defer cancel()
	}
	dswp.pods.RLock()
	retryState := dswp.pods.initialVolumeRetries[uniquePodName]
	allowInitialRetry = allowInitialRetry && !retryState.finished
	dswp.pods.RUnlock()

	allVolumesAdded := true
	onlyAuthorizationErrors := true
	var firstFailure time.Time
	var authorizationErrors []string
	publishAuthorizationErrors := func() {
		for _, err := range authorizationErrors {
			dswp.desiredStateOfWorld.AddErrorToPodWithBackoffStart(uniquePodName, err, retryState.firstFailure)
		}
		authorizationErrors = nil
	}
	collectSELinuxOptions := utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod)
	mounts, devices, seLinuxContainerContexts := util.GetPodVolumeNames(pod, collectSELinuxOptions)

	// Process volume spec for each volume defined in pod
	for _, podVolume := range pod.Spec.Volumes {
		if !mounts.Has(podVolume.Name) && !devices.Has(podVolume.Name) {
			// Volume is not used in the pod, ignore it.
			logger.V(4).Info("Skipping unused volume", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
			continue
		}

		pvc, volumeSpec, volumeGIDValue, err :=
			dswp.createVolumeSpec(retryCtx, podVolume, pod, mounts, devices)
		if err != nil {
			if errors.Is(err, errInitialVolumeRetryCapacity) {
				publishAuthorizationErrors()
				dswp.failInitialVolumeRetry(ctx, uniquePodName, "shared client capacity unavailable", retryState.firstFailure)
				return
			}
			observedAt := dswp.clock.Now()
			logger.Error(err, "Error processing volume", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
			if allowInitialRetry && onlyAuthorizationErrors && apierrors.IsForbidden(err) {
				if firstFailure.IsZero() {
					firstFailure = observedAt
				}
				authorizationErrors = append(authorizationErrors, err.Error())
			} else {
				onlyAuthorizationErrors = false
				publishAuthorizationErrors()
				if errors.Is(err, context.DeadlineExceeded) && retryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
					dswp.desiredStateOfWorld.AddErrorToPodWithBackoffStart(uniquePodName, err.Error(), retryState.firstFailure)
				} else {
					dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
				}
			}
			allVolumesAdded = false
			continue
		}

		// Add volume to desired state of world
		uniqueVolumeName, err := dswp.desiredStateOfWorld.AddPodToVolume(
			logger, uniquePodName, pod, volumeSpec, podVolume.Name, volumeGIDValue, seLinuxContainerContexts[podVolume.Name])
		if err != nil {
			logger.Error(err, "Failed to add volume to desiredStateOfWorld", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "volumeSpecName", volumeSpec.Name())
			publishAuthorizationErrors()
			dswp.desiredStateOfWorld.AddErrorToPod(uniquePodName, err.Error())
			onlyAuthorizationErrors = false
			allVolumesAdded = false
			continue
		}

		logger.V(4).Info("Added volume to desired state", "pod", klog.KObj(pod), "volumeName", podVolume.Name, "volumeSpecName", volumeSpec.Name())
		dswp.checkVolumeFSResize(logger, pod, podVolume, pvc, volumeSpec, uniqueVolumeName)
	}

	// some of the volume additions may have failed, should not mark this pod as fully processed
	if allVolumesAdded {
		dswp.markPodProcessed(uniquePodName)
		// New pod has been synced. Re-mount all volumes that need it
		// (e.g. DownwardAPI)
		dswp.actualStateOfWorld.MarkRemountRequired(logger, uniquePodName)
		// Remove any stored errors for the pod, everything went well in this processPodVolumes
		dswp.desiredStateOfWorld.PopPodErrors(uniquePodName)
	} else {
		// Binding and NodeAuthorizer graph updates are asynchronous. Defer only
		// bounded initial lookup failures, not errors from steady-state refreshes.
		if allowInitialRetry && onlyAuthorizationErrors && len(authorizationErrors) > 0 &&
			dswp.deferInitialVolumeErrors(uniquePodName, authorizationErrors, firstFailure) {
			return
		}
		publishAuthorizationErrors()
		if retryCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			for _, err := range retryState.errors {
				dswp.desiredStateOfWorld.AddErrorToPodWithBackoffStart(uniquePodName, err, retryState.firstFailure)
			}
		}
		if !dswp.podHasBeenSeenOnce(uniquePodName) {
			return
		}
		// For the Pod which has been processed at least once, even though some volumes
		// may not have been reprocessed successfully this round, we still mark it as processed to avoid
		// processing it at a very high frequency. The pod will be reprocessed when volume manager calls
		// ReprocessPod() which is triggered by SyncPod.
		dswp.markPodProcessed(uniquePodName)
	}

}

func (dswp *desiredStateOfWorldPopulator) initialVolumeRetryContext(
	ctx context.Context, podName volumetypes.UniquePodName) (context.Context, context.CancelFunc, bool) {
	dswp.pods.Lock()
	state, found := dswp.pods.initialVolumeRetries[podName]
	if !found || state.finished {
		dswp.pods.Unlock()
		return ctx, nil, true
	}
	now := dswp.clock.Now()
	if state.exhausted(now) {
		dswp.pods.Unlock()
		dswp.failInitialVolumeRetry(ctx, podName, "retry budget exhausted", state.firstFailure)
		return ctx, nil, false
	}
	if now.Before(state.nextRetry) {
		dswp.pods.Unlock()
		return ctx, nil, false
	}
	state.passes++
	dswp.pods.initialVolumeRetries[podName] = state
	dswp.pods.Unlock()
	retryCtx, cancel := context.WithTimeout(ctx, state.deadline.Sub(now))
	return context.WithValue(retryCtx, initialVolumeRetryContextKey{}, true), cancel, true
}

func (dswp *desiredStateOfWorldPopulator) failInitialVolumeRetry(ctx context.Context, podName volumetypes.UniquePodName, reason string, backoffStart time.Time) {
	dswp.pods.Lock()
	state, found := dswp.pods.initialVolumeRetries[podName]
	if !found || state.finished {
		dswp.pods.Unlock()
		return
	}
	dswp.pods.processedPods[podName] = true
	dswp.pods.initialVolumeRetries[podName] = initialVolumeRetryState{finished: true}
	dswp.pods.Unlock()
	for _, err := range state.errors {
		dswp.desiredStateOfWorld.AddErrorToPodWithBackoffStart(podName, err, backoffStart)
	}
	klog.FromContext(ctx).V(3).Info("Stopped initial volume authorization retries", "podUID", podName, "retryPasses", state.passes, "reason", reason)
}

func (dswp *desiredStateOfWorldPopulator) deferInitialVolumeErrors(podName volumetypes.UniquePodName, errs []string, firstFailure time.Time) bool {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()
	state, found := dswp.pods.initialVolumeRetries[podName]
	if state.finished {
		return false
	}
	now := dswp.clock.Now()
	if !found {
		state.firstFailure = firstFailure
		state.deadline = now.Add(initialVolumeRetryTimeout)
	}
	if state.exhausted(now) {
		return false
	}
	state.errors = append([]string(nil), errs...)
	state.nextRetry = now.Add(wait.Jitter(initialVolumeRetryDelay<<state.passes, initialVolumeRetryJitter))
	dswp.pods.processedPods[podName] = false
	dswp.pods.initialVolumeRetries[podName] = state
	return true
}

// checkVolumeFSResize records desired PVC size for a volume mounted by the pod.
// It is used for comparison with actual size(coming from pvc.Status.Capacity) and calling
// volume expansion on the node if needed.
func (dswp *desiredStateOfWorldPopulator) checkVolumeFSResize(
	logger klog.Logger,
	pod *v1.Pod,
	podVolume v1.Volume,
	pvc *v1.PersistentVolumeClaim,
	volumeSpec *volume.Spec,
	uniqueVolumeName v1.UniqueVolumeName) {

	// if a volumeSpec does not have PV or has InlineVolumeSpecForCSIMigration set or pvc is nil
	// we can't resize the volume and hence resizing should be skipped.
	if volumeSpec.PersistentVolume == nil || volumeSpec.InlineVolumeSpecForCSIMigration || pvc == nil {
		// Only PVC supports resize operation.
		return
	}

	// volumeSpec.ReadOnly is the value that determines if volume could be formatted when being mounted.
	// This is the same flag that determines filesystem resizing behaviour for offline resizing and hence
	// we should use it here. This value comes from Pod.spec.volumes.persistentVolumeClaim.readOnly.
	if volumeSpec.ReadOnly {
		// This volume is used as read only by this pod, we don't perform resize for read only volumes.
		logger.V(5).Info("Skip file system resize check for the volume, as the volume is mounted as readonly", "pod", klog.KObj(pod), "volumeName", podVolume.Name)
		return
	}

	pvCap := volumeSpec.PersistentVolume.Spec.Capacity.Storage().DeepCopy()
	pvcStatusCap := pvc.Status.Capacity.Storage().DeepCopy()
	dswp.desiredStateOfWorld.UpdatePersistentVolumeSize(uniqueVolumeName, pvCap)
	logger.V(5).Info("NodeExpandVolume updating size", "actualSize", pvcStatusCap.String(), "desiredSize", pvCap.String(), "volumeName", uniqueVolumeName)
	// in case the actualStateOfWorld was rebuild after kubelet restart ensure that claimSize is set to accurate value
	dswp.actualStateOfWorld.InitializeClaimSize(logger, uniqueVolumeName, pvcStatusCap)
}

// podPreviouslyProcessed returns true if the volumes for this pod have already
// been processed/reprocessed by the populator. Otherwise, the volumes for this pod need to
// be reprocessed.
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()

	return dswp.pods.processedPods[podName]
}

// markPodProcessingFailed marks the specified pod from processedPods as false to indicate that it failed processing
func (dswp *desiredStateOfWorldPopulator) markPodProcessingFailed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	dswp.pods.processedPods[podName] = false
	dswp.pods.Unlock()
}

// podHasBeenSeenOnce returns true if the pod has been seen by the popoulator
// at least once.
func (dswp *desiredStateOfWorldPopulator) podHasBeenSeenOnce(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	_, exist := dswp.pods.processedPods[podName]
	dswp.pods.RUnlock()
	return exist
}

// markPodProcessed records that the volumes for the specified pod have been
// processed by the populator
func (dswp *desiredStateOfWorldPopulator) markPodProcessed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	dswp.pods.processedPods[podName] = true
	// ReprocessPod must not renew this budget after success or failure escalation.
	dswp.pods.initialVolumeRetries[podName] = initialVolumeRetryState{finished: true}
}

// deleteProcessedPod removes the specified pod from processedPods
func (dswp *desiredStateOfWorldPopulator) deleteProcessedPod(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	delete(dswp.pods.processedPods, podName)
	delete(dswp.pods.initialVolumeRetries, podName)
}

// createVolumeSpec creates and returns a mutable volume.Spec object for the
// specified volume. It dereference any PVC to get PV objects, if needed.
// Returns an error if unable to obtain the volume at this time.
func (dswp *desiredStateOfWorldPopulator) createVolumeSpec(
	ctx context.Context, podVolume v1.Volume, pod *v1.Pod, mounts, devices sets.Set[string]) (*v1.PersistentVolumeClaim, *volume.Spec, string, error) {
	logger := klog.FromContext(ctx)
	pvcSource := podVolume.VolumeSource.PersistentVolumeClaim
	isEphemeral := pvcSource == nil && podVolume.VolumeSource.Ephemeral != nil
	if isEphemeral {
		// Generic ephemeral inline volumes are handled the
		// same way as a PVC reference. The only additional
		// constraint (checked below) is that the PVC must be
		// owned by the pod.
		pvcSource = &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: ephemeral.VolumeClaimName(pod, &podVolume),
		}
	}
	if pvcSource != nil {
		logger.V(5).Info("Found PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName))
		// If podVolume is a PVC, fetch the real PV behind the claim
		pvc, err := dswp.getPVCExtractPV(
			ctx, pod.Namespace, pvcSource.ClaimName)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %s/%s: %w",
				pod.Namespace,
				pvcSource.ClaimName,
				err)
		}
		if isEphemeral {
			if err := ephemeral.VolumeIsForPod(pod, pvc); err != nil {
				return nil, nil, "", err
			}
		}
		pvName, pvcUID := pvc.Spec.VolumeName, pvc.UID
		logger.V(5).Info("Found bound PV for PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName), "PVCUID", pvcUID, "PVName", pvName)
		// Fetch actual PV object
		volumeSpec, volumeGIDValue, err :=
			dswp.getPVSpec(ctx, pvName, pvcSource.ReadOnly, pvcUID)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %s/%s: %w",
				pod.Namespace,
				pvcSource.ClaimName,
				err)
		}
		logger.V(5).Info("Extracted volumeSpec from bound PV and PVC", "PVC", klog.KRef(pod.Namespace, pvcSource.ClaimName), "PVCUID", pvcUID, "PVName", pvName, "volumeSpecName", volumeSpec.Name())
		migratable, err := dswp.csiMigratedPluginManager.IsMigratable(volumeSpec)
		if err != nil {
			return nil, nil, "", err
		}
		if migratable {
			volumeSpec, err = csimigration.TranslateInTreeSpecToCSI(logger, volumeSpec, pod.Namespace, dswp.intreeToCSITranslator)
			if err != nil {
				return nil, nil, "", err
			}
		}

		volumeMode, err := util.GetVolumeMode(volumeSpec)
		if err != nil {
			return nil, nil, "", err
		}
		// Error if a container has volumeMounts but the volumeMode of PVC isn't Filesystem.
		if mounts.Has(podVolume.Name) && volumeMode != v1.PersistentVolumeFilesystem {
			return nil, nil, "", fmt.Errorf(
				"volume %s has volumeMode %s, but is specified in volumeMounts",
				podVolume.Name,
				volumeMode)
		}
		// Error if a container has volumeDevices but the volumeMode of PVC isn't Block
		if devices.Has(podVolume.Name) && volumeMode != v1.PersistentVolumeBlock {
			return nil, nil, "", fmt.Errorf(
				"volume %s has volumeMode %s, but is specified in volumeDevices",
				podVolume.Name,
				volumeMode)
		}
		return pvc, volumeSpec, volumeGIDValue, nil
	}

	// Do not return the original volume object, since the source could mutate it
	clonedPodVolume := podVolume.DeepCopy()

	spec := volume.NewSpecFromVolume(clonedPodVolume)
	migratable, err := dswp.csiMigratedPluginManager.IsMigratable(spec)
	if err != nil {
		return nil, nil, "", err
	}
	if migratable {
		spec, err = csimigration.TranslateInTreeSpecToCSI(logger, spec, pod.Namespace, dswp.intreeToCSITranslator)
		if err != nil {
			return nil, nil, "", err
		}
	}
	return nil, spec, "", nil
}

// getPVCExtractPV fetches the PVC object with the given namespace and name from
// the API server, checks whether PVC is being deleted, extracts the name of the PV
// it is pointing to and returns it.
// An error is returned if the PVC object's phase is not "Bound".
func (dswp *desiredStateOfWorldPopulator) getPVCExtractPV(
	ctx context.Context, namespace string, claimName string) (*v1.PersistentVolumeClaim, error) {
	pvc, err :=
		dswp.volumeClient(ctx).PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch PVC from API server: %w", err)
	}
	if pvc == nil {
		return nil, fmt.Errorf("API server returned no PVC %s/%s", namespace, claimName)
	}

	// Pods that uses a PVC that is being deleted and not protected by
	// kubernetes.io/pvc-protection must not be started.
	//
	// 1) In case an old kubelet is running without this check, the worst
	// that can happen is that such pod is scheduled. This was the default
	// behavior in 1.8 and earlier and users should not be that surprised.
	// It should happen only in very rare case when scheduler schedules
	// a pod and user deletes a PVC that's used by it at the same time.
	//
	// 2) Adding a check for kubernetes.io/pvc-protection here to prevent
	// the existing running pods from being affected during the rebuild of
	// the desired state of the world cache when the kubelet is restarted.
	// It is safe for kubelet to add this check here because the PVC will
	// be stuck in Terminating state until the pod is deleted.
	if pvc.ObjectMeta.DeletionTimestamp != nil && !slices.Contains(pvc.Finalizers, util.PVCProtectionFinalizer) {
		return nil, errors.New("PVC is being deleted")
	}

	if pvc.Status.Phase != v1.ClaimBound {
		return nil, errors.New("PVC is not bound")
	}
	if pvc.Spec.VolumeName == "" {
		return nil, errors.New("PVC has empty pvc.Spec.VolumeName")
	}

	return pvc, nil
}

// getPVSpec fetches the PV object with the given name from the API server
// and returns a volume.Spec representing it.
// An error is returned if the call to fetch the PV object fails.
func (dswp *desiredStateOfWorldPopulator) getPVSpec(
	ctx context.Context,
	name string,
	pvcReadOnly bool,
	expectedClaimUID types.UID) (*volume.Spec, string, error) {
	pv, err := dswp.volumeClient(ctx).PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, "", fmt.Errorf(
			"failed to fetch PV %s from API server: %w", name, err)
	}
	if pv == nil {
		return nil, "", fmt.Errorf("API server returned no PV %s", name)
	}

	if pv.Spec.ClaimRef == nil {
		return nil, "", fmt.Errorf(
			"found PV object %s but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim",
			name)
	}

	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, "", fmt.Errorf(
			"found PV object %s but its pv.Spec.ClaimRef.UID %s does not point to claim.UID %s",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}

	volumeGIDValue := getPVVolumeGidAnnotationValue(pv)
	return volume.NewSpecFromPersistentVolume(pv, pvcReadOnly), volumeGIDValue, nil
}

func getPVVolumeGidAnnotationValue(pv *v1.PersistentVolume) string {
	if volumeGid, ok := pv.Annotations[util.VolumeGidAnnotationKey]; ok {
		return volumeGid
	}

	return ""
}
