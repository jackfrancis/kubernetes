/*
Copyright 2017 The Kubernetes Authors.

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

package populator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/util/flowcontrol"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/ktesting"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

const (
	Removed     string = "removed"
	Terminating string = "terminating"
	Other       string = "other"
)

func pluginPVOmittingClient(dswp *desiredStateOfWorldPopulator) {
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return false, nil, nil
	})
	fakeClient.AddReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		return false, nil, nil
	})
	dswp.kubeClient = fakeClient
}

func prepareDswpWithVolume(t *testing.T) (*desiredStateOfWorldPopulator, kubepod.Manager, *fakePodStateProvider) {
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, fakePodStateProvider := createDswpWithVolume(t, pv, pvc)
	return dswp, fakePodManager, fakePodStateProvider
}

func TestFindAndAddNewPods_WithDifferentConditions(t *testing.T) {
	tests := []struct {
		desc          string
		hasAddedPods  bool
		podState      string
		expectedFound bool // Found pod is added to DSW
	}{
		{
			desc:          "HasAddedPods is false, ShouldPodRuntimeBeRemoved and ShouldPodContainerBeTerminating are both true",
			hasAddedPods:  false,
			podState:      Removed,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is false, ShouldPodRuntimeBeRemoved is false, ShouldPodContainerBeTerminating is true",
			hasAddedPods:  false,
			podState:      Terminating,
			expectedFound: true, // Pod should be added to DSW
		},
		{
			desc:          "HasAddedPods is false, other condition",
			hasAddedPods:  false,
			podState:      Other,
			expectedFound: true, // Pod should be added to DSW
		},
		{
			desc:          "HasAddedPods is true, ShouldPodRuntimeBeRemoved is false, ShouldPodContainerBeTerminating is true",
			hasAddedPods:  true,
			podState:      Terminating,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is true, ShouldPodRuntimeBeRemoved and ShouldPodContainerBeTerminating are both true",
			hasAddedPods:  true,
			podState:      Removed,
			expectedFound: false, // Pod should not be added to DSW
		},
		{
			desc:          "HasAddedPods is true, other condition",
			hasAddedPods:  true,
			podState:      Other,
			expectedFound: true, // Pod should be added to DSW
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// create dswp
			dswp, fakePodManager, fakePodState := prepareDswpWithVolume(t)

			// create pod
			containers := []v1.Container{
				{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "dswp-test-volume-name",
							MountPath: "/mnt",
						},
					},
				},
			}
			pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

			fakePodManager.AddPod(pod)

			switch tc.podState {
			case Removed:
				fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}
			case Terminating:
				fakePodState.terminating = map[kubetypes.UID]struct{}{pod.UID: {}}
			case Other:
				break
			}

			dswp.hasAddedPods = tc.hasAddedPods
			// Action
			tCtx := ktesting.Init(t)
			dswp.findAndAddNewPods(tCtx)

			// Verify
			podsInDSW := dswp.desiredStateOfWorld.GetPods()
			found := false
			if podsInDSW[types.UniquePodName(pod.UID)] {
				found = true
			}

			if found != tc.expectedFound {
				t.Fatalf(
					"Pod with uid %v has expectedFound value %v in pods in DSW %v",
					pod.UID, tc.expectedFound, podsInDSW)
			}
		})
	}
}

func TestFindAndAddNewPods_WithReprocessPodAndVolumeRetrievalError(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		failure  string
	}{
		{name: "PVC/forbidden", resource: "persistentvolumeclaims", failure: "forbidden"},
		{name: "PV/forbidden", resource: "persistentvolumes", failure: "forbidden"},
		{name: "PVC/not_found", resource: "persistentvolumeclaims", failure: "not_found"},
		{name: "PV/not_found", resource: "persistentvolumes", failure: "not_found"},
		{name: "PVC/deleting", resource: "persistentvolumeclaims", failure: "deleting"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, _, pod, volumeName, _ := prepareDSWPWithPodPV(t)
			podName := util.GetUniquePodName(pod)
			client := dswp.kubeClient.(*fake.Clientset)
			client.ClearActions()

			getAttempts := 0
			failLookup := true
			client.PrependReactor("get", tc.resource, func(action core.Action) (bool, runtime.Object, error) {
				getAttempts++
				if !failLookup {
					return false, nil, nil
				}
				name := action.(core.GetAction).GetName()
				switch tc.failure {
				case "forbidden":
					return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
				case "not_found":
					return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), name)
				case "deleting":
					now := metav1.Now()
					return true, &v1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:              name,
							Namespace:         action.GetNamespace(),
							DeletionTimestamp: &now,
						},
						Spec: v1.PersistentVolumeClaimSpec{VolumeName: "dswp-test-volume-name"},
						Status: v1.PersistentVolumeClaimStatus{
							Phase: v1.ClaimBound,
						},
					}, nil
				default:
					t.Fatalf("unsupported lookup failure %q", tc.failure)
					return true, nil, nil
				}
			})

			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts)
			require.True(t, dswp.podPreviouslyProcessed(podName))
			require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors())
			require.True(t, dswp.desiredStateOfWorld.PodExistsInVolume(podName, volumeName, ""))
			actionCount := len(client.Actions())

			// A failed refresh must not cause another API lookup on each populator pass.
			for range 3 {
				dswp.findAndAddNewPods(tCtx)
			}
			require.Len(t, client.Actions(), actionCount)

			if tc.failure == "forbidden" {
				failLookup = false
				dswp.findAndAddNewPods(tCtx)
				require.Len(t, client.Actions(), actionCount, "authorization recovery alone must not bypass refresh throttling")
			}

			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 2, getAttempts, "a new pod sync must permit another lookup")
			require.True(t, dswp.podPreviouslyProcessed(podName))
			require.True(t, dswp.desiredStateOfWorld.PodExistsInVolume(podName, volumeName, ""))
			if tc.failure == "forbidden" {
				require.Empty(t, dswp.desiredStateOfWorld.PopPodErrors(podName))
			} else {
				require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors())
			}
		})
	}
}

func TestFindAndAddNewPods_WithVolumeRetrievalError(t *testing.T) {
	// create dswp
	dswp, fakePodManager, _ := prepareDswpWithVolume(t)

	pluginPVOmittingClient(dswp)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if dswp.podPreviouslyProcessed(podName) {
		t.Fatalf("The volumes for the specified pod: %s should not have been processed by the populator", podName)
	}
	if dswp.podHasBeenSeenOnce(podName) {
		t.Fatalf("The volumes for the specified pod: %s should not have been processed by the populator", podName)
	}
}

func TestFindAndAddNewPods_TransientVolumeAuthorizationError(t *testing.T) {
	tests := []struct {
		name         string
		resource     string
		reprocessPod bool
	}{
		{name: "PVC/before_initial_pod_sync", resource: "persistentvolumeclaims"},
		{name: "PVC/during_initial_pod_sync", resource: "persistentvolumeclaims", reprocessPod: true},
		{name: "PV/before_initial_pod_sync", resource: "persistentvolumes"},
		{name: "PV/during_initial_pod_sync", resource: "persistentvolumes", reprocessPod: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dswp-test-claim",
					Namespace: "dswp-test",
					UID:       "dswp-test-claim-uid",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeName: "dswp-test-volume",
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase: v1.ClaimBound,
				},
			}
			pv := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName},
				Spec: v1.PersistentVolumeSpec{
					ClaimRef: &v1.ObjectReference{
						Namespace: pvc.Namespace,
						Name:      pvc.Name,
						UID:       pvc.UID,
					},
					VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
					PersistentVolumeSource: v1.PersistentVolumeSource{
						HostPath: &v1.HostPathVolumeSource{Path: "/fake-volume"},
					},
				},
			}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dswp-test-pod",
					Namespace: pvc.Namespace,
					UID:       "dswp-test-pod-uid",
				},
				Spec: v1.PodSpec{
					NodeName: "fake",
					Containers: []v1.Container{{
						Name:         "container",
						VolumeMounts: []v1.VolumeMount{{Name: "volume", MountPath: "/mnt"}},
					}},
					Volumes: []v1.Volume{{
						Name: "volume",
						VolumeSource: v1.VolumeSource{
							PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
						},
					}},
				},
			}

			dswp, podManager, dsw, _, _ := createDswpWithVolume(t, pv, pvc)
			clock := clocktesting.NewFakeClock(time.Unix(1, 0))
			dswp.clock = clock
			podManager.AddPod(pod)
			podName := util.GetUniquePodName(pod)
			getAttempts := 0
			dswp.kubeClient.(*fake.Clientset).PrependReactor("get", tc.resource, func(action core.Action) (bool, runtime.Object, error) {
				getAttempts++
				if getAttempts == 1 {
					return true, nil, apierrors.NewForbidden(
						action.GetResource().GroupResource(),
						action.(core.GetAction).GetName(),
						fmt.Errorf("node: no relationship found between node %q and this object", pod.Spec.NodeName))
				}
				return false, nil, nil
			})

			// WaitForAttachAndMount can request reprocessing before the first populator pass.
			if tc.reprocessPod {
				dswp.ReprocessPod(podName)
			}
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts)
			require.Empty(t, dsw.GetVolumesToMount())
			require.Empty(t, dsw.GetPodsWithErrors(), "a transient initial denial must not abort the volume wait")

			// Authorization has recovered, but no new pod sync requests reprocessing.
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts, "the next populator pass must respect retry spacing")
			clock.Step(time.Second)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 2, getAttempts, "initial volume processing must retry a transient authorization error without another ReprocessPod call")
			volumes := dsw.GetVolumesToMount()
			require.Len(t, volumes, 1)
			require.Equal(t, podName, volumes[0].PodName)
			require.Equal(t, pv.Name, volumes[0].VolumeSpec.Name())
			require.Empty(t, dsw.PopPodErrors(podName), "successful volume processing must clear the authorization error")
		})
	}
}

type orderedVolumePriorityPods struct {
	kubepod.Manager
	pods []*v1.Pod
}

func (p *orderedVolumePriorityPods) GetPods() []*v1.Pod { return p.pods }

func prepareVolumeRetryPods(t *testing.T, names ...string) (*desiredStateOfWorldPopulator, *orderedVolumePriorityPods, *clocktesting.FakeClock, *fakePodStateProvider) {
	t.Helper()
	dswp, podManager, podState := prepareDswpWithVolume(t)
	clock := clocktesting.NewFakeClock(time.Unix(1, 0))
	dswp.clock = clock
	manager := &orderedVolumePriorityPods{Manager: podManager}
	var objects []runtime.Object
	for _, name := range names {
		pod := createPodWithVolume(name, "volume", name+"-claim", []v1.Container{{
			VolumeMounts: []v1.VolumeMount{{Name: "volume", MountPath: "/mnt"}},
		}})
		pod.Spec.Volumes[0].GCEPersistentDisk = nil
		claim := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-claim", Namespace: pod.Namespace, UID: kubetypes.UID(name + "-claim-uid")},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: name + "-pv"},
			Status:     v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
		}
		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName},
			Spec: v1.PersistentVolumeSpec{
				ClaimRef:   &v1.ObjectReference{Namespace: claim.Namespace, Name: claim.Name, UID: claim.UID},
				VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/" + name},
				},
			},
		}
		objects = append(objects, claim, pv)
		manager.AddPod(pod)
		manager.pods = append(manager.pods, pod)
	}
	dswp.podManager = manager
	dswp.kubeClient = fake.NewSimpleClientset(objects...)
	return dswp, manager, clock, podState
}

func BenchmarkVolumePopulatorProcessedPods(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("pods=%d", count), func(b *testing.B) {
			pods := make([]*v1.Pod, count)
			dswp := &desiredStateOfWorldPopulator{
				podManager:       &orderedVolumePriorityPods{pods: pods},
				podStateProvider: &fakePodStateProvider{},
				hasAddedPods:     true,
				clock:            clock.RealClock{},
				pods: processedPods{
					processedPods:        make(map[types.UniquePodName]bool, count),
					initialVolumeRetries: make(map[types.UniquePodName]initialVolumeRetryState, count),
				},
			}
			for i := range pods {
				uid := fmt.Sprintf("pod-%d", i)
				pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: kubetypes.UID(uid)}}
				dswp.pods.processedPods[types.UniquePodName(uid)] = true
				dswp.pods.initialVolumeRetries[types.UniquePodName(uid)] = initialVolumeRetryState{finished: true}
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dswp.findAndAddNewPods(ctx)
			}
		})
	}
}

func TestInitialVolumeAuthorizationRetryPriority(t *testing.T) {
	for _, scenario := range []string{
		"new_pod", "ordinary_refresh", "new_denial",
		"expired_before_scan", "expires_during_fresh_work",
		"terminates_during_fresh_work", "runtime_removed_during_fresh_work",
		"deleted_during_fresh_work", "canceled_during_fresh_work",
	} {
		t.Run(scenario, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			ctx, cancel := context.WithCancel(tCtx)
			defer cancel()
			dswp, manager, clock, podState := prepareVolumeRetryPods(t, "retry", "fresh")
			podManager, pods := manager.Manager, manager.pods
			retryPod, freshPod := pods[0], pods[1]
			retryName, freshName := util.GetUniquePodName(retryPod), util.GetUniquePodName(freshPod)
			manager.pods = pods[:1]
			client := dswp.kubeClient.(*fake.Clientset)
			initialPass := true
			var requests []string
			client.PrependReactor("get", "*", func(action core.Action) (bool, runtime.Object, error) {
				resource, name := action.GetResource().Resource, action.(core.GetAction).GetName()
				requests = append(requests, resource+"/"+name)
				if resource != "persistentvolumeclaims" {
					return false, nil, nil
				}
				if initialPass && name == "retry-claim" {
					return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
				}
				if initialPass {
					return false, nil, nil
				}
				if name == "fresh-claim" {
					switch scenario {
					case "new_denial":
						return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
					case "expired_before_scan":
						require.Equal(t, []types.UniquePodName{retryName}, dswp.desiredStateOfWorld.GetPodsWithErrors(),
							"already-expired errors must be published before starting fresh lookups")
					case "expires_during_fresh_work":
						clock.Step(3 * time.Second)
					case "terminates_during_fresh_work":
						podState.terminating = map[kubetypes.UID]struct{}{retryPod.UID: {}}
					case "runtime_removed_during_fresh_work":
						podState.removed = map[kubetypes.UID]struct{}{retryPod.UID: {}}
					case "deleted_during_fresh_work":
						podManager.RemovePod(retryPod)
					case "canceled_during_fresh_work":
						cancel()
					}
				} else if name == "retry-claim" && scenario == "new_denial" {
					// A newly deferred failure becomes due during the old retry, but
					// must not receive a second lookup pass in this same iteration.
					clock.Step(200 * time.Millisecond)
				}
				return false, nil, nil
			})
			firstDenial := clock.Now()
			dswp.ReprocessPod(retryName)
			dswp.findAndAddNewPods(ctx)
			require.Equal(t, []string{"persistentvolumeclaims/retry-claim"}, requests)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
			if scenario == "ordinary_refresh" {
				dswp.processPodVolumes(ctx, freshPod)
				require.True(t, dswp.podPreviouslyProcessed(freshName))
				dswp.ReprocessPod(freshName)
			}
			initialPass = false
			requests = nil
			manager.pods = pods
			clock.Step(200 * time.Millisecond)
			if scenario == "expired_before_scan" {
				clock.Step(3 * time.Second)
			}
			dswp.hasAddedPods = scenario == "terminates_during_fresh_work"
			dswp.findAndAddNewPods(ctx)

			expected := []string{"persistentvolumeclaims/fresh-claim", "persistentvolumes/fresh-pv"}
			switch scenario {
			case "new_denial":
				expected = expected[:1]
				require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
				fallthrough
			case "new_pod", "ordinary_refresh":
				expected = append(expected, "persistentvolumeclaims/retry-claim", "persistentvolumes/retry-pv")
			case "expired_before_scan", "expires_during_fresh_work":
				started, deferred := kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(retryName)...))
				require.True(t, deferred)
				require.Equal(t, firstDenial, started, "priority must not restart the fallback clock")
			case "terminates_during_fresh_work", "runtime_removed_during_fresh_work":
				errs := dswp.desiredStateOfWorld.PopPodErrors(retryName)
				require.NotEmpty(t, errs, "stopping startup retries must flush their errors")
				_, deferred := kubecontainer.LatestBackoffStart(errors.Join(errs...))
				require.False(t, deferred, "termination must not inherit startup timing")
			}
			require.Equal(t, expected, requests, "ordinary work must precede optional retries, which must recheck eligibility")
		})
	}
}

func TestInitialVolumeAuthorizationRetryQuantum(t *testing.T) {
	for _, sameDueTime := range []bool{false, true} {
		t.Run(fmt.Sprintf("sameDueTime=%t", sameDueTime), func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, manager, clock, _ := prepareVolumeRetryPods(t, "pod-c", "pod-b", "pod-a", "fresh")
			retryPods, fresh := manager.pods[:3], manager.pods[3]
			manager.RemovePod(fresh)
			manager.pods = retryPods
			initial, arrived := true, false
			var requests []string
			dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "*", func(action core.Action) (bool, runtime.Object, error) {
				resource, name := action.GetResource().Resource, action.(core.GetAction).GetName()
				requests = append(requests, resource+"/"+name)
				if resource != "persistentvolumeclaims" || name == "fresh-claim" {
					return false, nil, nil
				}
				if !initial && !arrived {
					arrived = true
					manager.AddPod(fresh)
					manager.pods = append(manager.pods, fresh)
				}
				return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
			})
			for _, pod := range retryPods {
				dswp.ReprocessPod(util.GetUniquePodName(pod))
			}
			dswp.findAndAddNewPods(tCtx)
			require.Len(t, requests, 3)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
			initial = false
			clock.Step(200 * time.Millisecond)
			expectedOrder := []string{"pod-b", "pod-c", "pod-a"}
			if sameDueTime {
				expectedOrder = []string{"pod-a", "pod-b", "pod-c"}
			}
			for i, name := range expectedOrder {
				podName := types.UniquePodName(name + "-uid")
				state := dswp.pods.initialVolumeRetries[podName]
				state.nextRetry = clock.Now().Add(time.Duration(i-3) * time.Millisecond)
				if sameDueTime {
					state.nextRetry = clock.Now()
				}
				dswp.pods.initialVolumeRetries[podName] = state
			}
			for i, name := range expectedOrder {
				requests = nil
				if i > 0 {
					// Input order changes independently of due order.
					manager.pods = []*v1.Pod{retryPods[2], retryPods[0], retryPods[1], fresh}
				}
				dswp.findAndAddNewPods(tCtx)
				expected := []string{}
				if i == 1 {
					expected = append(expected, "persistentvolumeclaims/fresh-claim", "persistentvolumes/fresh-pv")
				}
				expected = append(expected, "persistentvolumeclaims/"+name+"-claim")
				require.Equal(t, expected, requests, "only the oldest due retry may run before obtaining another Pod snapshot")
			}
			require.True(t, dswp.podPreviouslyProcessed(util.GetUniquePodName(fresh)))
			for _, pod := range retryPods {
				require.Equal(t, 1, dswp.pods.initialVolumeRetries[util.GetUniquePodName(pod)].passes,
					"an earlier retry must not monopolize later admission opportunities")
			}
		})
	}
}

func TestInitialVolumeAuthorizationRetryQuantumControlWork(t *testing.T) {
	for _, terminateDuringFresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminateDuringFresh=%t", terminateDuringFresh), func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, manager, clock, podState := prepareVolumeRetryPods(t, "expired", "waiting", "reconstruct", "due", "stopped", "fresh")
			pods := manager.pods
			manager.pods = pods[:5]
			initial := true
			var requests []string
			dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "*", func(action core.Action) (bool, runtime.Object, error) {
				resource, name := action.GetResource().Resource, action.(core.GetAction).GetName()
				requests = append(requests, resource+"/"+name)
				if resource != "persistentvolumeclaims" {
					return false, nil, nil
				}
				if !initial && name == "fresh-claim" {
					podState.terminating = map[kubetypes.UID]struct{}{pods[2].UID: {}}
					return false, nil, nil
				}
				if !initial && name == "reconstruct-claim" {
					return false, nil, nil
				}
				return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
			})
			for _, pod := range manager.pods {
				dswp.ReprocessPod(util.GetUniquePodName(pod))
			}
			firstDenial := clock.Now()
			dswp.findAndAddNewPods(tCtx)
			require.Len(t, requests, 5)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
			initial, requests = false, nil
			clock.Step(200 * time.Millisecond)
			expiredName := util.GetUniquePodName(pods[0])
			state := dswp.pods.initialVolumeRetries[expiredName]
			state.deadline = clock.Now()
			dswp.pods.initialVolumeRetries[expiredName] = state
			waitingName := util.GetUniquePodName(pods[1])
			state = dswp.pods.initialVolumeRetries[waitingName]
			state.nextRetry = clock.Now().Add(time.Second)
			dswp.pods.initialVolumeRetries[waitingName] = state
			podState.removed = map[kubetypes.UID]struct{}{pods[4].UID: {}}
			if terminateDuringFresh {
				manager.pods = pods
			} else {
				podState.terminating = map[kubetypes.UID]struct{}{pods[2].UID: {}}
			}
			dswp.findAndAddNewPods(tCtx)
			expected := []string{}
			if terminateDuringFresh {
				expected = append(expected, "persistentvolumeclaims/fresh-claim", "persistentvolumes/fresh-pv")
			}
			expected = append(expected, "persistentvolumeclaims/reconstruct-claim", "persistentvolumes/reconstruct-pv", "persistentvolumeclaims/due-claim")
			require.Equal(t, expected, requests, "expiry, not-yet-due work, and reconstruction must not consume the retry quantum")
			started, deferred := kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(expiredName)...))
			require.True(t, deferred)
			require.Equal(t, firstDenial, started)
			errs := dswp.desiredStateOfWorld.PopPodErrors(util.GetUniquePodName(pods[4]))
			require.NotEmpty(t, errs)
			_, deferred = kubecontainer.LatestBackoffStart(errors.Join(errs...))
			require.False(t, deferred, "cleanup must not inherit startup timing")
			require.Equal(t, 0, dswp.pods.initialVolumeRetries[waitingName].passes)
			require.Equal(t, 1, dswp.pods.initialVolumeRetries[util.GetUniquePodName(pods[3])].passes)
		})
	}
}

func TestInitialVolumeAuthorizationRetryBudget(t *testing.T) {
	for _, resource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		for _, expire := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expire=%t", resource, expire), func(t *testing.T) {
				tCtx := ktesting.Init(t)
				dswp, podManager, _ := prepareDswpWithVolume(t)
				clock := clocktesting.NewFakeClock(time.Unix(1, 0))
				dswp.clock = clock
				pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{{
					VolumeMounts: []v1.VolumeMount{{Name: "dswp-test-volume-name", MountPath: "/mnt"}},
				}})
				podManager.AddPod(pod)
				podName := util.GetUniquePodName(pod)
				getAttempts := 0
				dswp.kubeClient.(*fake.Clientset).PrependReactor("get", resource, func(action core.Action) (bool, runtime.Object, error) {
					getAttempts++
					return true, nil, apierrors.NewForbidden(
						action.GetResource().GroupResource(), action.(core.GetAction).GetName(), fmt.Errorf("no relationship found"))
				})
				dswp.ReprocessPod(podName)
				firstFailure := clock.Now()
				dswp.findAndAddNewPods(tCtx)
				require.Equal(t, 1, getAttempts)
				require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())

				if expire {
					clock.Step(3 * time.Second)
					dswp.findAndAddNewPods(tCtx)
					require.Equal(t, 1, getAttempts, "the time budget must expire without another request")
				} else {
					for i, step := range []time.Duration{200 * time.Millisecond, 300 * time.Millisecond, 500 * time.Millisecond, time.Second} {
						clock.Step(step)
						dswp.findAndAddNewPods(tCtx)
						require.Equal(t, i+2, getAttempts)
						if i < 3 {
							require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
						}
					}
					require.Equal(t, 5, getAttempts, "only four additional initial lookup passes are allowed")
				}
				require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors())
				require.Empty(t, dswp.desiredStateOfWorld.GetVolumesToMount())
				started, deferred := kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(podName)...))
				require.True(t, deferred)
				require.Equal(t, firstFailure, started, "exhaustion must preserve the original denial time")
				attemptsAtExhaustion := getAttempts
				clock.Step(time.Hour)
				dswp.findAndAddNewPods(tCtx)
				require.Equal(t, attemptsAtExhaustion, getAttempts)

				dswp.ReprocessPod(podName)
				dswp.findAndAddNewPods(tCtx)
				require.Equal(t, attemptsAtExhaustion+1, getAttempts)
				require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors())
				_, deferred = kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(podName)...))
				require.False(t, deferred, "an ordinary resync must not reuse an old failure's timing")
				clock.Step(time.Hour)
				dswp.findAndAddNewPods(tCtx)
				require.Equal(t, attemptsAtExhaustion+1, getAttempts, "ordinary resync must not renew the retry budget")
			})
		}
	}
}

func TestInitialVolumeAuthorizationFallbackStartsAtFirstDenial(t *testing.T) {
	tCtx := ktesting.Init(t)
	dswp, podManager, _ := prepareDswpWithVolume(t)
	clock := clocktesting.NewFakeClock(time.Unix(1, 0))
	dswp.clock = clock
	pod := createPodWithVolume("pod", "volume", "file-bound", []v1.Container{{
		VolumeMounts: []v1.VolumeMount{{Name: "volume", MountPath: "/mnt"}, {Name: "other", MountPath: "/other"}},
	}})
	other := pod.Spec.Volumes[0].DeepCopy()
	other.Name = "other"
	other.PersistentVolumeClaim.ClaimName = "other"
	pod.Spec.Volumes = append(pod.Spec.Volumes, *other)
	podManager.AddPod(pod)
	podName := util.GetUniquePodName(pod)
	getAttempts := 0
	dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		getAttempts++
		name := action.(core.GetAction).GetName()
		if name == "other" {
			clock.Step(5 * time.Second)
		}
		return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
	})
	firstFailure := clock.Now()
	dswp.ReprocessPod(podName)
	dswp.findAndAddNewPods(tCtx)
	require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors(), "the existing retry window still starts after the initial pass")
	clock.Step(3 * time.Second)
	dswp.findAndAddNewPods(tCtx)
	require.Equal(t, 2, getAttempts, "exhaustion must not issue another request")
	started, deferred := kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(podName)...))
	require.True(t, deferred)
	require.Equal(t, firstFailure, started, "time spent on remaining volumes must count toward normal fallback")
}

type volumeRetryTestTransport func(*http.Request) (*http.Response, error)

func (f volumeRetryTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type volumeRetryAdmissionLimiter struct {
	flowcontrol.RateLimiter
	waits    atomic.Int32
	tries    atomic.Int32
	rejected atomic.Int32
}

func (l *volumeRetryAdmissionLimiter) Wait(ctx context.Context) error {
	l.waits.Add(1)
	return l.RateLimiter.Wait(ctx)
}

func (l *volumeRetryAdmissionLimiter) TryAccept() bool {
	l.tries.Add(1)
	accepted := l.RateLimiter.TryAccept()
	if !accepted {
		l.rejected.Add(1)
	}
	return accepted
}

func TestInitialVolumeAuthorizationRetryCapacity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		blocked       string
		tokens        int
		initialWaits  int
		tries         int
		rejected      int
		expectedReads []string
		freshFailure  bool
	}{
		{name: "PVC retry refused", blocked: "PVC", tokens: 1, initialWaits: 1, tries: 1, rejected: 1, expectedReads: []string{"PVC"}},
		{name: "PV retry refused after PVC", blocked: "PV", tokens: 3, initialWaits: 2, tries: 2, rejected: 1, expectedReads: []string{"PVC", "PV", "PVC"}},
		{name: "PVC recovery charges once", blocked: "PVC", tokens: 3, initialWaits: 1, tries: 2, expectedReads: []string{"PVC", "PVC", "PV"}},
		{name: "PV recovery charges once", blocked: "PV", tokens: 4, initialWaits: 2, tries: 2, expectedReads: []string{"PVC", "PV", "PVC", "PV"}},
		{name: "fresh failure before refusal", blocked: "PVC", tokens: 3, initialWaits: 2, tries: 2, rejected: 1, expectedReads: []string{"PVC", "PVC", "PVC"}, freshFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			ctx, cancel := context.WithTimeout(tCtx, 5*time.Second)
			defer cancel()
			dswp, manager, clock, _ := prepareVolumeRetryPods(t, "pod")
			pod := manager.pods[0]
			podName := util.GetUniquePodName(pod)
			pvc, err := dswp.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, "pod-claim", metav1.GetOptions{})
			require.NoError(t, err)
			pv, err := dswp.kubeClient.CoreV1().PersistentVolumes().Get(ctx, "pod-pv", metav1.GetOptions{})
			require.NoError(t, err)
			pvc.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}
			pv.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"}
			if tc.freshFailure {
				second := pod.Spec.Volumes[0].DeepCopy()
				second.Name = "second"
				second.PersistentVolumeClaim.ClaimName = "second-claim"
				pod.Spec.Volumes = append(pod.Spec.Volumes, *second)
				pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, v1.VolumeMount{Name: "second", MountPath: "/second"})
			}
			// Zero refill makes exact token charging independent of wall time.
			limiter := &volumeRetryAdmissionLimiter{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(0, tc.tokens)}
			initial := true
			var reads []string
			client, err := clientset.NewForConfig(&rest.Config{
				Host: "https://volume-retry.invalid", RateLimiter: limiter,
				Transport: volumeRetryTestTransport(func(request *http.Request) (*http.Response, error) {
					var object runtime.Object
					var resource string
					name := pvc.Name
					switch request.URL.Path {
					case "/api/v1/namespaces/" + pod.Namespace + "/persistentvolumeclaims/pod-claim":
						object, resource = pvc, "PVC"
					case "/api/v1/persistentvolumes/pod-pv":
						object, resource = pv, "PV"
						name = pv.Name
					case "/api/v1/namespaces/" + pod.Namespace + "/persistentvolumeclaims/second-claim":
						object, resource, name = pvc, "PVC", "second-claim"
					default:
						t.Fatalf("unexpected volume request %s", request.URL.Path)
					}
					reads = append(reads, resource)
					code := http.StatusOK
					if initial && resource == tc.blocked {
						kind := "persistentvolumeclaims"
						if resource == "PV" {
							kind = "persistentvolumes"
						}
						status := apierrors.NewForbidden(v1.Resource(kind), name, fmt.Errorf("no relationship found")).ErrStatus
						status.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
						object, code = &status, http.StatusForbidden
					} else if tc.freshFailure && resource == "PVC" {
						status := apierrors.NewNotFound(v1.Resource("persistentvolumeclaims"), name).ErrStatus
						status.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
						object, code = &status, http.StatusNotFound
					}
					body, err := json.Marshal(object)
					require.NoError(t, err)
					return &http.Response{
						StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}},
						Body: io.NopCloser(bytes.NewReader(body)), Request: request,
					}, nil
				}),
			})
			require.NoError(t, err)
			dswp.kubeClient = client
			dswp.ReprocessPod(podName)
			firstDenial := clock.Now()
			dswp.findAndAddNewPods(ctx)
			require.Len(t, reads, tc.initialWaits)
			require.EqualValues(t, tc.initialWaits, limiter.waits.Load())
			require.Zero(t, limiter.tries.Load(), "an ordinary request deadline must not select retry admission")
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())

			initial = false
			clock.Step(200 * time.Millisecond)
			dswp.findAndAddNewPods(ctx)
			require.Equal(t, tc.expectedReads, reads)
			require.EqualValues(t, tc.initialWaits, limiter.waits.Load(), "fast retries must not queue a normal limiter wait")
			require.EqualValues(t, tc.tries, limiter.tries.Load())
			require.EqualValues(t, tc.rejected, limiter.rejected.Load())
			require.False(t, limiter.RateLimiter.TryAccept(), "every actual request must consume exactly one shared token")
			if tc.rejected > 0 {
				err := errors.Join(dswp.desiredStateOfWorld.PopPodErrors(podName)...)
				require.ErrorContains(t, err, "is forbidden: no relationship found")
				require.NotContains(t, err.Error(), "rate limiter", "local admission refusal must retain the real authorization failure")
				started, deferred := kubecontainer.LatestBackoffStart(err)
				require.Equal(t, !tc.freshFailure, deferred)
				if tc.freshFailure {
					require.ErrorContains(t, err, "not found", "admission refusal must not hide a fresh failure")
				} else {
					require.Equal(t, firstDenial, started)
				}
				require.False(t, kubecontainer.IsBackoffError(err))
			} else {
				require.Len(t, dswp.desiredStateOfWorld.GetVolumesToMount(), 1)
				require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
			}
			clock.Step(time.Second)
			dswp.findAndAddNewPods(ctx)
			require.EqualValues(t, tc.tries, limiter.tries.Load(), "refusal or recovery must close the accelerated episode")
			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(ctx)
			require.EqualValues(t, tc.initialWaits+len(pod.Spec.Volumes), limiter.waits.Load(), "ordinary resync must retain normal client throttling")
			require.EqualValues(t, tc.tries, limiter.tries.Load(), "ordinary resync must not renew accelerated admission")
			errs := dswp.desiredStateOfWorld.PopPodErrors(podName)
			require.NotEmpty(t, errs)
			_, deferred := kubecontainer.LatestBackoffStart(errors.Join(errs...))
			require.False(t, deferred, "a fresh ordinary failure must not reuse startup timing")
		})
	}
}

func TestInitialVolumeRetryAdmissionCancellation(t *testing.T) {
	dswp, manager, clock, _ := prepareVolumeRetryPods(t, "pod")
	limiter := flowcontrol.NewTokenBucketRateLimiter(0, 1)
	client, err := clientset.NewForConfig(&rest.Config{
		Host: "https://volume-retry.invalid", RateLimiter: limiter,
		Transport: volumeRetryTestTransport(func(request *http.Request) (*http.Response, error) {
			t.Errorf("canceled retry reached HTTP transport: %s", request.URL)
			return nil, context.Canceled
		}),
	})
	require.NoError(t, err)
	dswp.kubeClient = client
	pod := manager.pods[0]
	podName := util.GetUniquePodName(pod)
	dswp.pods.initialVolumeRetries[podName] = initialVolumeRetryState{
		deadline: clock.Now().Add(time.Second), nextRetry: clock.Now(),
	}
	ctx, cancel, process := dswp.initialVolumeRetryContext(t.Context(), podName)
	require.True(t, process)
	require.NotNil(t, cancel)
	cancel()
	_, err = dswp.getPVCExtractPV(ctx, pod.Namespace, "pod-claim")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, limiter.TryAccept(), "a canceled retry must not consume a shared token")
}

func TestInitialVolumeAuthorizationRetryRequestDeadline(t *testing.T) {
	tCtx := ktesting.Init(t)
	ctx, cancel := context.WithTimeout(tCtx, 5*time.Second)
	defer cancel()
	dswp, podManager, _ := prepareDswpWithVolume(t)
	clock := clocktesting.NewFakeClock(time.Unix(1, 0))
	dswp.clock = clock
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{{
		VolumeMounts: []v1.VolumeMount{{Name: "dswp-test-volume-name", MountPath: "/mnt"}},
	}})
	podManager.AddPod(pod)
	podName := util.GetUniquePodName(pod)
	var getAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if getAttempts.Add(1) == 1 {
			status := apierrors.NewForbidden(v1.Resource("persistentvolumeclaims"), "file-bound", fmt.Errorf("no relationship found")).ErrStatus
			status.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			if err := json.NewEncoder(w).Encode(status); err != nil {
				t.Errorf("writing initial authorization denial: %v", err)
			}
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	var requestBudgets []time.Duration
	client, err := clientset.NewForConfig(&rest.Config{
		Host: server.URL, QPS: -1,
		WrapTransport: func(base http.RoundTripper) http.RoundTripper {
			return volumeRetryTestTransport(func(request *http.Request) (*http.Response, error) {
				deadline, ok := request.Context().Deadline()
				require.True(t, ok)
				requestBudgets = append(requestBudgets, time.Until(deadline))
				return base.RoundTrip(request)
			})
		},
	})
	require.NoError(t, err)
	dswp.kubeClient = client
	dswp.ReprocessPod(podName)
	firstFailure := clock.Now()
	dswp.findAndAddNewPods(ctx)
	require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())

	// The API call must inherit the remaining budget, not the manager's lifetime.
	clock.Step(2 * time.Second)
	dswp.findAndAddNewPods(ctx)
	require.EqualValues(t, 2, getAttempts.Load())
	require.Len(t, requestBudgets, 2)
	require.LessOrEqual(t, requestBudgets[1], time.Second, "the retry must use the remaining one second, not the five-second parent")
	errs := dswp.desiredStateOfWorld.PopPodErrors(podName)
	require.Contains(t, fmt.Sprint(errs), context.DeadlineExceeded.Error())
	require.Contains(t, fmt.Sprint(errs), "no relationship found")
	combined := errors.Join(errs...)
	started, deferred := kubecontainer.LatestBackoffStart(combined)
	require.True(t, deferred)
	require.Equal(t, firstFailure, started)
	require.False(t, errors.Is(combined, context.DeadlineExceeded), "retry expiry must remain an ordinary volume failure")
	clock.Step(time.Hour)
	dswp.findAndAddNewPods(tCtx)
	require.EqualValues(t, 2, getAttempts.Load(), "an expired request must end the fast-retry episode")
}

func TestInitialVolumeAuthorizationRetryTerminatingPod(t *testing.T) {
	for _, reconstruct := range []bool{false, true} {
		t.Run(fmt.Sprintf("reconstruct=%t", reconstruct), func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, podManager, podState := prepareDswpWithVolume(t)
			clock := clocktesting.NewFakeClock(time.Unix(1, 0))
			dswp.clock = clock
			pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{{
				VolumeMounts: []v1.VolumeMount{{Name: "dswp-test-volume-name", MountPath: "/mnt"}},
			}})
			podManager.AddPod(pod)
			podName := util.GetUniquePodName(pod)
			getAttempts := 0
			dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
				getAttempts++
				return true, nil, apierrors.NewForbidden(
					action.GetResource().GroupResource(), action.(core.GetAction).GetName(), fmt.Errorf("no relationship found"))
			})
			dswp.ReprocessPod(podName)
			if reconstruct {
				podState.terminating = map[kubetypes.UID]struct{}{pod.UID: {}}
			} else {
				dswp.hasAddedPods = true
				dswp.findAndAddNewPods(tCtx)
				require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
				podState.terminating = map[kubetypes.UID]struct{}{pod.UID: {}}
			}
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts)
			require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors(),
				"termination must not leave a suppressed error behind when startup retries stop")
			_, deferred := kubecontainer.LatestBackoffStart(errors.Join(dswp.desiredStateOfWorld.PopPodErrors(podName)...))
			require.False(t, deferred, "startup timing must not accelerate termination or reconstruction")
			dswp.hasAddedPods = true
			clock.Step(time.Hour)
			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts)
		})
	}
}

func TestReprocessedVolumeAuthorizationErrorIsReportedImmediately(t *testing.T) {
	tCtx := ktesting.Init(t)
	dswp, _, pod, _, _ := prepareDSWPWithPodPV(t)
	podName := util.GetUniquePodName(pod)
	other := pod.Spec.Volumes[0].DeepCopy()
	other.Name = "other"
	other.PersistentVolumeClaim.ClaimName = "other"
	pod.Spec.Volumes = append(pod.Spec.Volumes, *other)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
		v1.VolumeMount{Name: "other", MountPath: "/other"})
	dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		name := action.(core.GetAction).GetName()
		if name == "file-bound" {
			return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
		}
		require.Equal(t, []types.UniquePodName{podName}, dswp.desiredStateOfWorld.GetPodsWithErrors(),
			"an ordinary refresh error must be visible before processing the remaining volumes")
		return false, nil, nil
	})
	dswp.ReprocessPod(podName)
	dswp.findAndAddNewPods(tCtx)
}

func TestInitialVolumeAuthorizationRetryDoesNotHideOtherErrors(t *testing.T) {
	for _, afterDeferral := range []bool{false, true} {
		t.Run(fmt.Sprintf("afterDeferral=%t", afterDeferral), func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, podManager, _ := prepareDswpWithVolume(t)
			clock := clocktesting.NewFakeClock(time.Unix(1, 0))
			dswp.clock = clock
			pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{{
				VolumeMounts: []v1.VolumeMount{
					{Name: "dswp-test-volume-name", MountPath: "/mnt"},
					{Name: "missing", MountPath: "/missing"},
				},
			}})
			pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
				Name: "missing",
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "missing"},
				},
			})
			podManager.AddPod(pod)
			podName := util.GetUniquePodName(pod)
			getAttempts := 0
			missing := !afterDeferral
			dswp.kubeClient.(*fake.Clientset).PrependReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
				getAttempts++
				name := action.(core.GetAction).GetName()
				if name == "missing" && missing {
					return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), name)
				}
				return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), name, fmt.Errorf("no relationship found"))
			})
			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 2, getAttempts)
			if afterDeferral {
				require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
				missing = true
				clock.Step(200 * time.Millisecond)
				dswp.findAndAddNewPods(tCtx)
				require.Equal(t, 4, getAttempts)
			}
			errs := dswp.desiredStateOfWorld.PopPodErrors(podName)
			require.Len(t, errs, 2)
			require.Contains(t, fmt.Sprint(errs), "not found")
			require.Contains(t, fmt.Sprint(errs), "no relationship found")
			_, deferred := kubecontainer.LatestBackoffStart(errors.Join(errs...))
			require.False(t, deferred, "a fresh failure must retain its full normal backoff")
			attempts := getAttempts
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, attempts, getAttempts, "a non-authorization failure must retain normal error backoff")
		})
	}
}

func TestFindAndAddNewPods_VolumeAuthorizationErrorPodLifecycle(t *testing.T) {
	for _, resource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		t.Run(resource, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			dswp, podManager, podState := prepareDswpWithVolume(t)
			clock := clocktesting.NewFakeClock(time.Unix(1, 0))
			dswp.clock = clock
			pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{{
				VolumeMounts: []v1.VolumeMount{{Name: "dswp-test-volume-name", MountPath: "/mnt"}},
			}})
			podName := util.GetUniquePodName(pod)
			podManager.AddPod(pod)
			dswp.hasAddedPods = true

			authorized := false
			getAttempts := 0
			client := dswp.kubeClient.(*fake.Clientset)
			client.PrependReactor("get", resource, func(action core.Action) (bool, runtime.Object, error) {
				getAttempts++
				if !authorized {
					return true, nil, apierrors.NewForbidden(
						action.GetResource().GroupResource(), action.(core.GetAction).GetName(), fmt.Errorf("no relationship found"))
				}
				return false, nil, nil
			})

			dswp.ReprocessPod(podName)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 1, getAttempts)
			require.Empty(t, dswp.desiredStateOfWorld.GetVolumesToMount())
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())

			podState.terminating = map[kubetypes.UID]struct{}{pod.UID: {}}
			authorized = true
			clock.Step(time.Hour)
			dswp.ReprocessPod(podName)
			actionCount := len(client.Actions())
			for range 3 {
				dswp.findAndAddNewPods(tCtx)
			}
			require.Len(t, client.Actions(), actionCount, "a terminating pod must not restart volume lookups")

			podManager.RemovePod(pod)
			podState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}
			dswp.findAndRemoveDeletedPods(tCtx.Logger())
			dswp.findAndAddNewPods(tCtx)
			require.Len(t, client.Actions(), actionCount)
			require.False(t, dswp.podHasBeenSeenOnce(podName))
			require.NotContains(t, dswp.pods.initialVolumeRetries, podName)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())

			recreated := pod.DeepCopy()
			recreated.UID = "recreated-pod-uid"
			authorized = false
			podManager.AddPod(recreated)
			dswp.ReprocessPod(util.GetUniquePodName(recreated))
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 2, getAttempts)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors(), "a new UID must receive its own initial retry budget")
			authorized = true
			clock.Step(time.Second)
			dswp.findAndAddNewPods(tCtx)
			require.Equal(t, 3, getAttempts)
			volumes := dswp.desiredStateOfWorld.GetVolumesToMount()
			require.Len(t, volumes, 1)
			require.Equal(t, util.GetUniquePodName(recreated), volumes[0].PodName)
			require.NotContains(t, dswp.desiredStateOfWorld.GetPods(), podName)
			require.Empty(t, dswp.desiredStateOfWorld.GetPodsWithErrors())
		})
	}
}

type mutablePodManager interface {
	GetPodByName(string, string) (*v1.Pod, bool)
	RemovePod(*v1.Pod)
}

func TestFindAndAddNewPods_FindAndRemoveDeletedPods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, _ := prepareDSWPWithPodPV(t)
	podName := util.GetUniquePodName(pod)

	//let the pod be terminated
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	dswp.podManager.(mutablePodManager).RemovePod(pod)

	dswp.findAndRemoveDeletedPods(logger)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Pod should not been removed from desired state of world since pod state still thinks it exists")
	}

	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// the pod state is marked as removed, so here findAndRemoveDeletedPods() will remove the pod and volumes it is mounted
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	// podWorker may call volume_manager WaitForUnmount() after we processed the pod in findAndRemoveDeletedPods()
	dswp.ReprocessPod(podName)
	dswp.findAndRemoveDeletedPods(logger)

	// findAndRemoveDeletedPods() above must detect orphaned pod and delete it from the map
	if _, ok := dswp.pods.processedPods[podName]; ok {
		t.Fatalf("Failed to remove orphanded pods from internal map")
	}

	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := dswp.desiredStateOfWorld.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

func TestFindAndRemoveDeletedPodsWithActualState(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, _ := prepareDSWPWithPodPV(t)
	fakeASW := dswp.actualStateOfWorld
	podName := util.GetUniquePodName(pod)

	//let the pod be terminated
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed

	dswp.findAndRemoveDeletedPods(logger)
	// Although Pod status is terminated, pod still exists in pod manager and actual state does not has this pod and volume information
	// desired state populator will fail to delete this pod and volume first
	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	// reconcile with actual state so that volume is added into the actual state
	// desired state populator now can successfully delete the pod and volume
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)
	dswp.findAndRemoveDeletedPods(logger)
	if !dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */) {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// reconcile with actual state so that volume is added into the actual state
	// desired state populator now can successfully delete the pod and volume
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)
	dswp.findAndRemoveDeletedPods(logger)
	volumeExists = dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}
}

func TestFindAndRemoveDeletedPodsWithUncertain(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	dswp, fakePodState, pod, expectedVolumeName, pv := prepareDSWPWithPodPV(t)
	podName := util.GetUniquePodName(pod)

	//let the pod be terminated
	podGet, exist := dswp.podManager.(mutablePodManager).GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	dswp.podManager.(mutablePodManager).RemovePod(pod)
	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	// Add the volume to ASW by reconciling.
	fakeASW := dswp.actualStateOfWorld
	reconcileASW(fakeASW, dswp.desiredStateOfWorld, t)

	// Mark the volume as uncertain
	opts := operationexecutor.MarkVolumeOpts{
		PodName:          util.GetUniquePodName(pod),
		PodUID:           pod.UID,
		VolumeName:       expectedVolumeName,
		VolumeGIDVolume:  "",
		VolumeSpec:       volume.NewSpecFromPersistentVolume(pv, false),
		VolumeMountState: operationexecutor.VolumeMountUncertain,
	}
	err := dswp.actualStateOfWorld.MarkVolumeMountAsUncertain(opts)
	if err != nil {
		t.Fatalf("Failed to set the volume as uncertain: %s", err)
	}

	// the pod state now lists the pod as removed, so here findAndRemoveDeletedPods() will remove the pod and volumes it is mounted
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	volumeExists := dswp.desiredStateOfWorld.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := dswp.desiredStateOfWorld.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := dswp.desiredStateOfWorld.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

func prepareDSWPWithPodPV(t *testing.T) (*desiredStateOfWorldPopulator, *fakePodStateProvider, *v1.Pod, v1.UniqueVolumeName, *v1.PersistentVolume) {
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, fakesDSW, _, fakePodState := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)
	return dswp, fakePodState, pod, expectedVolumeName, pv
}

func TestFindAndRemoveNonattachableVolumes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	fakeVolumePluginMgr, fakeVolumePlugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
	dswp, fakePodManager, fakesDSW, _, _ := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumesToMount := fakesDSW.GetVolumesToMount()
	if len(volumesToMount) != 1 {
		t.Fatalf("Expected one volume in DSW, got %d", len(volumesToMount))
	}

	err := dswp.actualStateOfWorld.MarkVolumeAsAttached(
		logger, expectedVolumeName, volumesToMount[0].VolumeSpec, "", "")
	if err != nil {
		t.Fatalf("Failed to add volume to ASW: %v", err)
	}

	attachedVolume, found := dswp.actualStateOfWorld.GetAttachedVolume(expectedVolumeName)
	if !found {
		t.Fatalf("Expected volume %q in ASW", expectedVolumeName)
	}
	if !attachedVolume.PluginIsAttachable {
		t.Fatalf("Expected volume %q to be attachable in ASW", expectedVolumeName)
	}

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	// change the volume plugin from attachable to non-attachable
	fakeVolumePlugin.NonAttachable = true

	// The volume should still exist
	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)

	dswp.findAndRemoveDeletedPods(logger)
	// After the volume plugin changes to nonattachable, the corresponding volume attachable field should change.
	volumesToMount = fakesDSW.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			if volume.PluginIsAttachable {
				t.Fatalf(
					"Volume %v in the list of desired state of world volumes to mount is still attachable. Expected not",
					expectedVolumeName)
			}
		}
	}
	attachedVolume, found = dswp.actualStateOfWorld.GetAttachedVolume(expectedVolumeName)
	if !found {
		t.Fatalf("Expected volume %q to remain in ASW", expectedVolumeName)
	}
	if attachedVolume.PluginIsAttachable {
		t.Fatalf("Expected volume %q to become non-attachable in ASW", expectedVolumeName)
	}
}

func TestEphemeralVolumeOwnerCheck(t *testing.T) {
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pod, pv, pvc := createEphemeralVolumeObjects("dswp-test-pod", "dswp-test-volume-name", false /* not owned */, &mode)
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)
	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("%s should not have been processed by the populator", podName)
	}
	errs := dswp.desiredStateOfWorld.PopPodErrors(podName)
	require.Len(t, errs, 1)
	require.EqualError(t, errs[0], fmt.Sprintf("PVC %s/%s was not created for pod %s/%s (pod is not owner)",
		pvc.Namespace, pvc.Name, pod.Namespace, pod.Name))
}

func TestFindAndAddNewPods_FindAndRemoveDeletedPods_Valid_Block_VolumeDevices(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// create dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, fakesDSW, _, fakePodState := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)

	podName := util.GetUniquePodName(pod)

	generatedVolumeName := "fake-plugin/" + pod.Spec.Volumes[0].Name

	tCtx := ktesting.Init(t)
	dswp.findAndAddNewPods(tCtx)

	if !dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to record that the volumes for the specified pod: %s have been processed by the populator", podName)
	}

	expectedVolumeName := v1.UniqueVolumeName(generatedVolumeName)

	volumeExists := fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if !volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); !podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
			podExistsInVolume)
	}

	verifyVolumeExistsInVolumesToMount(
		t, v1.UniqueVolumeName(generatedVolumeName), false /* expectReportedInUse */, fakesDSW)

	//let the pod be terminated
	podGet, exist := fakePodManager.GetPodByName(pod.Namespace, pod.Name)
	if !exist {
		t.Fatalf("Failed to get pod by pod name: %s and namespace: %s", pod.Name, pod.Namespace)
	}
	podGet.Status.Phase = v1.PodFailed
	fakePodManager.RemovePod(pod)
	fakePodState.removed = map[kubetypes.UID]struct{}{pod.UID: {}}

	//pod is added to fakePodManager but pod state knows the pod is removed, so here findAndRemoveDeletedPods() will remove the pod and volumes it is mounted
	dswp.findAndRemoveDeletedPods(logger)
	if dswp.pods.processedPods[podName] {
		t.Fatalf("Failed to remove pods from desired state of world since they no longer exist")
	}

	volumeExists = fakesDSW.VolumeExists(expectedVolumeName, "" /* SELinuxContext */)
	if volumeExists {
		t.Fatalf(
			"VolumeExists(%q) failed. Expected: <false> Actual: <%v>",
			expectedVolumeName,
			volumeExists)
	}

	if podExistsInVolume := fakesDSW.PodExistsInVolume(
		podName, expectedVolumeName, "" /* SELinuxContext */); podExistsInVolume {
		t.Fatalf(
			"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
			podExistsInVolume)
	}

	volumesToMount := fakesDSW.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			t.Fatalf(
				"Found volume %v in the list of desired state of world volumes to mount. Expected not",
				expectedVolumeName)
		}
	}
}

func TestCreateVolumeSpec_Valid_File_VolumeMounts(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
			VolumeMode: &mode,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// Assert
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of filesystem mode and volumeMounts. err: %v", err)
	}
}

func TestCreateVolumeSpec_Valid_Nil_VolumeMounts(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// create dswp
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
			VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// Assert
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of filesystem mode and volumeMounts. err: %v", err)
	}
}

func TestCreateVolumeSpec_Valid_Block_VolumeDevices(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// create dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// Assert
	if volumeSpec == nil || err != nil {
		t.Fatalf("Failed to create volumeSpec with combination of block mode and volumeDevices. err: %v", err)
	}
}

func TestCreateVolumeSpec_Invalid_File_VolumeDevices(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// create dswp
	mode := v1.PersistentVolumeFilesystem
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeDevices: []v1.VolumeDevice{
				{
					Name:       "dswp-test-volume-name",
					DevicePath: "/dev/sdb",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(ctx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// Assert
	if volumeSpec != nil || err == nil {
		t.Fatalf("Unexpected volumeMode and volumeMounts/volumeDevices combination is accepted")
	}
}

func TestCreateVolumeSpec_Invalid_Block_VolumeMounts(t *testing.T) {
	tCtx := ktesting.Init(t)
	// create dswp
	mode := v1.PersistentVolumeBlock
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "block-bound"},
			VolumeMode: &mode,
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "dswp-test-volume-name",
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}
	dswp, fakePodManager, _, _, _ := createDswpWithVolume(t, pv, pvc)

	// create pod
	containers := []v1.Container{
		{
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "dswp-test-volume-name",
					MountPath: "/mnt",
				},
			},
		},
	}
	pod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "block-bound", containers)

	fakePodManager.AddPod(pod)
	mountsMap, devicesMap, _ := util.GetPodVolumeNames(pod, false /* collectSELinuxOptions */)
	_, volumeSpec, _, err :=
		dswp.createVolumeSpec(tCtx, pod.Spec.Volumes[0], pod, mountsMap, devicesMap)

	// Assert
	if volumeSpec != nil || err == nil {
		t.Fatalf("Unexpected volumeMode and volumeMounts/volumeDevices combination is accepted")
	}
}

func TestCheckVolumeFSResize(t *testing.T) {
	setCapacity := func(pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, capacity int) {
		pv.Spec.Capacity = volumeCapacity(capacity)
		pvc.Spec.Resources.Requests = volumeCapacity(capacity)
	}

	testcases := []struct {
		resize      func(*testing.T, *v1.PersistentVolume, *v1.PersistentVolumeClaim, *desiredStateOfWorldPopulator)
		verify      func(*testing.T, []v1.UniqueVolumeName, v1.UniqueVolumeName)
		readOnlyVol bool
		volumeMode  v1.PersistentVolumeMode
		volumeType  string
	}{
		{
			// No resize request for volume, volumes in ASW shouldn't be marked as fsResizeRequired
			resize: func(*testing.T, *v1.PersistentVolume, *v1.PersistentVolumeClaim, *desiredStateOfWorldPopulator) {
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("No resize request for any volumes, but found resize required volumes in ASW: %v", vols)
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},

		{
			// Make volume used as ReadOnly, so volume shouldn't be marked as fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("volume mounted as ReadOnly, but found resize required volumes in ASW: %v", vols)
				}
			},
			readOnlyVol: true,
			volumeMode:  v1.PersistentVolumeFilesystem,
		},
		{
			// Clear ASW, so volume shouldn't be marked as fsResizeRequired because they are not mounted
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, dswp *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
				clearASW(dswp.actualStateOfWorld, dswp.desiredStateOfWorld, t)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, _ v1.UniqueVolumeName) {
				if len(vols) > 0 {
					t.Errorf("volume hasn't been mounted, but found resize required volumes in ASW: %v", vols)
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			// volume in ASW should be marked as fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Request resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			// volume in ASW should be marked as fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Requested resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeBlock,
		},
		{
			// volume in ASW should be marked as fsResizeRequired
			resize: func(_ *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, _ *desiredStateOfWorldPopulator) {
				setCapacity(pv, pvc, 2)
			},
			verify: func(t *testing.T, vols []v1.UniqueVolumeName, volName v1.UniqueVolumeName) {
				if len(vols) == 0 {
					t.Fatalf("Requested resize for volume, but volume in ASW hasn't been marked as fsResizeRequired")
				}
				if len(vols) != 1 {
					t.Errorf("Some unexpected volumes are marked as fsResizeRequired: %v", vols)
				}
				if vols[0] != volName {
					t.Fatalf("Mark wrong volume as fsResizeRequired: %s", vols[0])
				}
			},
			volumeMode: v1.PersistentVolumeFilesystem,
			volumeType: "ephemeral",
		},
	}

	for _, tc := range testcases {
		var pod *v1.Pod
		var pvc *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume

		if tc.volumeType == "ephemeral" {
			pod, pv, pvc = createEphemeralVolumeObjects("dswp-test-pod", "dswp-test-volume-name", true, &tc.volumeMode)
		} else {
			pv, pvc = createResizeRelatedVolumes(&tc.volumeMode)
			containers := []v1.Container{}

			if tc.volumeMode == v1.PersistentVolumeFilesystem {
				containers = append(containers, v1.Container{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      pv.Name,
							MountPath: "/mnt",
							ReadOnly:  tc.readOnlyVol,
						},
					},
				})
			} else {
				containers = append(containers, v1.Container{
					VolumeDevices: []v1.VolumeDevice{
						{
							Name:       pv.Name,
							DevicePath: "/mnt/foobar",
						},
					},
				})
			}

			pod = createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", containers)
			pod.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim.ReadOnly = tc.readOnlyVol
		}

		dswp, fakePodManager, fakeDSW, _, _ := createDswpWithVolume(t, pv, pvc)
		fakeASW := dswp.actualStateOfWorld
		uniquePodName := types.UniquePodName(pod.UID)
		uniqueVolumeName := v1.UniqueVolumeName("fake-plugin/" + pod.Spec.Volumes[0].Name)

		fakePodManager.AddPod(pod)
		// Fill the dsw to contains volumes and pods.
		tCtx := ktesting.Init(t)
		dswp.findAndAddNewPods(tCtx)
		reconcileASW(fakeASW, fakeDSW, t)

		func() {
			tc.resize(t, pv, pvc, dswp)

			resizeRequiredVolumes := reprocess(tCtx, dswp, uniquePodName, fakeDSW, fakeASW, *pv.Spec.Capacity.Storage())

			tc.verify(t, resizeRequiredVolumes, uniqueVolumeName)
		}()
	}
}

func TestCheckVolumeSELinux(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMountReadWriteOncePod, true)
	fullOpts := &v1.SELinuxOptions{
		User:  "system_u",
		Role:  "object_r",
		Type:  "container_t",
		Level: "s0:c1,c2",
	}
	differentFullOpts := &v1.SELinuxOptions{
		User:  "system_u",
		Role:  "object_r",
		Type:  "container_t",
		Level: "s0:c9998,c9999",
	}
	partialOpts := &v1.SELinuxOptions{
		Level: "s0:c3,c4",
	}

	testcases := []struct {
		name                         string
		accessModes                  []v1.PersistentVolumeAccessMode
		existingContainerSELinuxOpts *v1.SELinuxOptions
		newContainerSELinuxOpts      *v1.SELinuxOptions
		seLinuxMountFeatureEnabled   bool
		pluginSupportsSELinux        bool
		expectError                  bool
		expectedContext              string
	}{
		{
			name:                    "RWOP with plugin with SELinux with full context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                    "RWOP with plugin with SELinux with partial context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: partialOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "system_u:object_r:container_file_t:s0:c3,c4",
		},
		{
			name:                    "RWX with plugin with SELinux with full context in pod and SELinuxMount feature disabled",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   true,
			expectedContext:         "", // RWX volumes don't support SELinux
		},
		{
			name:                       "RWX with plugin with SELinux with full context in pod and SELinuxMount feature enabled",
			accessModes:                []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			newContainerSELinuxOpts:    fullOpts,
			pluginSupportsSELinux:      true,
			seLinuxMountFeatureEnabled: true,
			expectedContext:            "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                    "RWOP with plugin with no SELinux with full context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: fullOpts,
			pluginSupportsSELinux:   false,
			expectedContext:         "", // plugin doesn't support SELinux
		},
		{
			name:                    "RWOP with plugin with SELinux with no context in pod",
			accessModes:             []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			newContainerSELinuxOpts: nil,
			pluginSupportsSELinux:   true,
			expectedContext:         "",
		},
		{
			name:                         "RWOP with plugin with SELinux with full context in pod with existing pod",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      fullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                         "mismatched SELinux with RWX - success",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "",
		},
		{
			name:                         "mismatched SELinux with RWX and SELinuxMount feature disabled",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectedContext:              "",
		},
		{
			name:                         "mismatched SELinux with RWX and SELinuxMount feature enabled",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			seLinuxMountFeatureEnabled:   true,
			expectError:                  true,
			// The original seLinuxOpts are kept in DSW
			expectedContext: "system_u:object_r:container_file_t:s0:c1,c2",
		},
		{
			name:                         "mismatched SELinux with RWOP - failure",
			accessModes:                  []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			existingContainerSELinuxOpts: fullOpts,
			newContainerSELinuxOpts:      differentFullOpts,
			pluginSupportsSELinux:        true,
			expectError:                  true,
			// The original seLinuxOpts are kept in DSW
			expectedContext: "system_u:object_r:container_file_t:s0:c1,c2",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			pv := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dswp-test-volume-name",
				},
				Spec: v1.PersistentVolumeSpec{
					VolumeMode:             ptr.To(v1.PersistentVolumeFilesystem),
					PersistentVolumeSource: v1.PersistentVolumeSource{RBD: &v1.RBDPersistentVolumeSource{}},
					Capacity:               volumeCapacity(1),
					ClaimRef:               &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
					AccessModes:            tc.accessModes,
				},
			}
			pvc := &v1.PersistentVolumeClaim{
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeMode: ptr.To(v1.PersistentVolumeFilesystem),
					VolumeName: pv.Name,
					Resources: v1.VolumeResourceRequirements{
						Requests: pv.Spec.Capacity,
					},
					AccessModes: tc.accessModes,
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase:    v1.ClaimBound,
					Capacity: pv.Spec.Capacity,
				},
			}

			container := v1.Container{
				SecurityContext: &v1.SecurityContext{
					SELinuxOptions: nil,
				},
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      pv.Name,
						MountPath: "/mnt",
					},
				},
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SELinuxMount, tc.seLinuxMountFeatureEnabled)

			fakeVolumePluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
			plugin.SupportsSELinux = tc.pluginSupportsSELinux
			dswp, fakePodManager, fakeDSW, _, _ := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)

			tCtx := ktesting.Init(t)
			var existingPod *v1.Pod
			if tc.existingContainerSELinuxOpts != nil {
				// Add existing pod + volume
				existingContainer := container
				existingContainer.SecurityContext.SELinuxOptions = tc.existingContainerSELinuxOpts
				existingPod = createPodWithVolume("dswp-old-pod", "dswp-test-volume-name", "file-bound", []v1.Container{existingContainer})
				fakePodManager.AddPod(existingPod)
				dswp.findAndAddNewPods(tCtx)
			}

			newContainer := container
			newContainer.SecurityContext.SELinuxOptions = tc.newContainerSELinuxOpts
			newPod := createPodWithVolume("dswp-test-pod", "dswp-test-volume-name", "file-bound", []v1.Container{newContainer})

			// Act - add the new Pod
			fakePodManager.AddPod(newPod)
			dswp.findAndAddNewPods(tCtx)

			// Assert

			// Check the global volume state
			uniquePodName := types.UniquePodName(newPod.UID)
			uniqueVolumeName := v1.UniqueVolumeName("fake-plugin/" + newPod.Spec.Volumes[0].Name)
			volumeExists := fakeDSW.VolumeExists(uniqueVolumeName, tc.expectedContext)
			if !volumeExists {
				t.Errorf(
					"VolumeExists(%q) failed. Expected: <true> Actual: <%v>",
					uniqueVolumeName,
					volumeExists)
			}

			// Check the Pod local volume state
			podExistsInVolume := fakeDSW.PodExistsInVolume(uniquePodName, uniqueVolumeName, tc.expectedContext)
			if !podExistsInVolume && !tc.expectError {
				t.Errorf(
					"DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <%v>",
					podExistsInVolume)
			}
			if podExistsInVolume && tc.expectError {
				t.Errorf(
					"DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <%v>",
					podExistsInVolume)
			}
			errors := fakeDSW.GetPodsWithErrors()
			if tc.expectError && len(errors) == 0 {
				t.Errorf("Expected Pod error, got none")
			}
			if !tc.expectError && len(errors) > 0 {
				t.Errorf("Unexpected Pod errors: %v", errors)
			}
			verifyVolumeExistsInVolumesToMount(t, uniqueVolumeName, false /* expectReportedInUse */, fakeDSW)
		})
	}
}

func createResizeRelatedVolumes(volumeMode *v1.PersistentVolumeMode) (pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) {
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dswp-test-volume-name",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{RBD: &v1.RBDPersistentVolumeSource{}},
			Capacity:               volumeCapacity(1),
			ClaimRef:               &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			VolumeMode:             volumeMode,
		},
	}
	pvc = &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: pv.Name,
			Resources: v1.VolumeResourceRequirements{
				Requests: pv.Spec.Capacity,
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: pv.Spec.Capacity,
		},
	}
	return
}

func volumeCapacity(size int) v1.ResourceList {
	return v1.ResourceList{v1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", size))}
}

func reconcileASW(asw cache.ActualStateOfWorld, dsw cache.DesiredStateOfWorld, t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		err := asw.MarkVolumeAsAttached(logger, volumeToMount.VolumeName, volumeToMount.VolumeSpec, "", "")
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsAttached: %v", err)
		}
		markVolumeOpts := operationexecutor.MarkVolumeOpts{
			PodName:          volumeToMount.PodName,
			PodUID:           volumeToMount.Pod.UID,
			VolumeName:       volumeToMount.VolumeName,
			VolumeGIDVolume:  volumeToMount.VolumeGIDValue,
			VolumeSpec:       volumeToMount.VolumeSpec,
			VolumeMountState: operationexecutor.VolumeMounted,
		}
		err = asw.MarkVolumeAsMounted(markVolumeOpts)
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsMounted: %v", err)
		}
	}
}

func clearASW(asw cache.ActualStateOfWorld, dsw cache.DesiredStateOfWorld, t *testing.T) {
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		err := asw.MarkVolumeAsUnmounted(volumeToMount.PodName, volumeToMount.VolumeName)
		if err != nil {
			t.Fatalf("Unexpected error when MarkVolumeAsUnmounted: %v", err)
		}
	}
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		asw.MarkVolumeAsDetached(volumeToMount.VolumeName, "")
	}
}

func reprocess(ctx context.Context, dswp *desiredStateOfWorldPopulator, uniquePodName types.UniquePodName,
	dsw cache.DesiredStateOfWorld, asw cache.ActualStateOfWorld, newSize resource.Quantity) []v1.UniqueVolumeName {
	logger := klog.FromContext(ctx)
	dswp.ReprocessPod(uniquePodName)
	dswp.findAndAddNewPods(ctx)
	return getResizeRequiredVolumes(logger, dsw, asw, newSize)
}

func getResizeRequiredVolumes(logger klog.Logger, dsw cache.DesiredStateOfWorld, asw cache.ActualStateOfWorld, newSize resource.Quantity) []v1.UniqueVolumeName {
	resizeRequiredVolumes := []v1.UniqueVolumeName{}
	for _, volumeToMount := range dsw.GetVolumesToMount() {
		_, _, err := asw.PodExistsInVolume(logger, volumeToMount.PodName, volumeToMount.VolumeName, newSize, "" /* SELinuxContext */)
		if cache.IsFSResizeRequiredError(err) {
			resizeRequiredVolumes = append(resizeRequiredVolumes, volumeToMount.VolumeName)
		}
	}
	return resizeRequiredVolumes
}

func verifyVolumeExistsInVolumesToMount(t *testing.T, expectedVolumeName v1.UniqueVolumeName, expectReportedInUse bool, dsw cache.DesiredStateOfWorld) {
	volumesToMount := dsw.GetVolumesToMount()
	for _, volume := range volumesToMount {
		if volume.VolumeName == expectedVolumeName {
			if volume.ReportedInUse != expectReportedInUse {
				t.Fatalf(
					"Found volume %v in the list of VolumesToMount, but ReportedInUse incorrect. Expected: <%v> Actual: <%v>",
					expectedVolumeName,
					expectReportedInUse,
					volume.ReportedInUse)
			}
			return
		}
	}

	t.Fatalf(
		"Could not find volume %v in the list of desired state of world volumes to mount %+v",
		expectedVolumeName,
		volumesToMount)
}

func createPodWithVolume(pod, pv, pvc string, containers []v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod,
			UID:       kubetypes.UID(pod + "-uid"),
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: pv,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "dswp-test-fake-device",
						},
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc,
						},
					},
				},
			},
			Containers: containers,
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}
}

func createEphemeralVolumeObjects(podName, volumeName string, owned bool, volumeMode *v1.PersistentVolumeMode) (pod *v1.Pod, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) {
	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			UID:       "dswp-test-pod-uid",
			Namespace: "dswp-test",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						Ephemeral: &v1.EphemeralVolumeSource{
							VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{},
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/mnt",
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodPhase("Running"),
		},
	}
	pv = &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeName,
		},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef:   &v1.ObjectReference{Namespace: "ns", Name: "file-bound"},
			Capacity:   volumeCapacity(1),
			VolumeMode: volumeMode,
		},
	}
	pvc = &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName + "-" + volumeName,
			Namespace: pod.Namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: volumeName,
			Resources: v1.VolumeResourceRequirements{
				Requests: pv.Spec.Capacity,
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:    v1.ClaimBound,
			Capacity: pv.Spec.Capacity,
		},
	}
	if owned {
		controller := true
		pvc.OwnerReferences = []metav1.OwnerReference{
			{
				UID:        pod.UID,
				Name:       podName,
				Controller: &controller,
			},
		}
	}
	return
}

func createDswpWithVolume(t *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) (*desiredStateOfWorldPopulator, kubepod.Manager, cache.DesiredStateOfWorld, *containertest.FakeRuntime, *fakePodStateProvider) {
	fakeVolumePluginMgr, _ := volumetesting.GetTestKubeletVolumePluginMgr(t)
	dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider := createDswpWithVolumeWithCustomPluginMgr(pv, pvc, fakeVolumePluginMgr)
	return dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider
}

type fakePodStateProvider struct {
	terminating map[kubetypes.UID]struct{}
	removed     map[kubetypes.UID]struct{}
}

func (p *fakePodStateProvider) ShouldPodContainersBeTerminating(uid kubetypes.UID) bool {
	_, ok := p.terminating[uid]
	// if ShouldPodRuntimeBeRemoved returns true, ShouldPodContainerBeTerminating should also return true
	if !ok {
		_, ok = p.removed[uid]
	}
	return ok
}

func (p *fakePodStateProvider) ShouldPodRuntimeBeRemoved(uid kubetypes.UID) bool {
	_, ok := p.removed[uid]
	return ok
}

func createDswpWithVolumeWithCustomPluginMgr(pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim,
	fakeVolumePluginMgr *volume.VolumePluginMgr) (*desiredStateOfWorldPopulator, kubepod.Manager, cache.DesiredStateOfWorld, *containertest.FakeRuntime, *fakePodStateProvider) {
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("get", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		return true, pvc, nil
	})
	fakeClient.AddReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		return true, pv, nil
	})

	fakePodManager := kubepod.NewBasicPodManager()

	seLinuxTranslator := util.NewFakeSELinuxLabelTranslator()
	fakesDSW := cache.NewDesiredStateOfWorld(fakeVolumePluginMgr, seLinuxTranslator)
	fakeASW := cache.NewActualStateOfWorld("fake", fakeVolumePluginMgr, nil)
	fakeRuntime := &containertest.FakeRuntime{}
	fakeStateProvider := &fakePodStateProvider{}

	csiTranslator := csitrans.New()
	dswp := &desiredStateOfWorldPopulator{
		kubeClient:          fakeClient,
		loopSleepDuration:   100 * time.Millisecond,
		podManager:          fakePodManager,
		podStateProvider:    fakeStateProvider,
		desiredStateOfWorld: fakesDSW,
		actualStateOfWorld:  fakeASW,
		pods: processedPods{
			processedPods:        make(map[types.UniquePodName]bool),
			initialVolumeRetries: make(map[types.UniquePodName]initialVolumeRetryState)},
		csiMigratedPluginManager: csimigration.NewPluginManager(csiTranslator),
		intreeToCSITranslator:    csiTranslator,
		volumePluginMgr:          fakeVolumePluginMgr,
		clock:                    clock.RealClock{},
	}
	return dswp, fakePodManager, fakesDSW, fakeRuntime, fakeStateProvider
}
