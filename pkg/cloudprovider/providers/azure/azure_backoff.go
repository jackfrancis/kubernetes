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

package azure

import (
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/compute"
	"github.com/Azure/azure-sdk-for-go/arm/network"
	"github.com/Azure/go-autorest/autorest"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// For API-driven retry flows, how many retry iterations?
const retryAfterIterations = 5

// GetVirtualMachineWithRetry invokes az.getVirtualMachine with configurable backoff retry
func (az *Cloud) GetVirtualMachineWithRetry(name types.NodeName) (compute.VirtualMachine, bool, error) {
	var machine compute.VirtualMachine
	var exists bool
	err := wait.ExponentialBackoff(az.resourceRequestBackoff, func() (bool, error) {
		var retryErr error
		machine, exists, retryErr = az.getVirtualMachine(name)
		if retryErr != nil {
			glog.Errorf("backoff: failure, will retry,err=%v", retryErr)
			return false, nil
		}
		glog.V(2).Infof("backoff: success")
		return true, nil
	})
	return machine, exists, err
}

// CreateOrUpdateSGWithRetry invokes az.SecurityGroupsClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateSGWithRetry(backoffConfig *wait.Backoff, sg network.SecurityGroup) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.SecurityGroupsClient.CreateOrUpdate(az.ResourceGroup, *sg.Name, sg, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdateLBWithRetry invokes az.LoadBalancerClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateLBWithRetry(backoffConfig *wait.Backoff, lb network.LoadBalancer) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.LoadBalancerClient.CreateOrUpdate(az.ResourceGroup, *lb.Name, lb, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdatePIPWithRetry invokes az.PublicIPAddressesClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdatePIPWithRetry(backoffConfig *wait.Backoff, pip network.PublicIPAddress) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.PublicIPAddressesClient.CreateOrUpdate(az.ResourceGroup, *pip.Name, pip, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdateInterfaceWithRetry invokes az.PublicIPAddressesClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateInterfaceWithRetry(backoffConfig *wait.Backoff, nic network.Interface) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.InterfacesClient.CreateOrUpdate(az.ResourceGroup, *nic.Name, nic, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// DeletePublicIPWithRetry invokes az.PublicIPAddressesClient.Delete with configurable backoff retry
func (az *Cloud) DeletePublicIPWithRetry(backoffConfig *wait.Backoff, pipName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.PublicIPAddressesClient.Delete(az.ResourceGroup, pipName, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// DeleteLBWithRetry invokes az.LoadBalancerClient.Delete with configurable backoff retry
func (az *Cloud) DeleteLBWithRetry(backoffConfig *wait.Backoff, lbName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.LoadBalancerClient.Delete(az.ResourceGroup, lbName, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdateRouteTableWithRetry invokes az.RouteTablesClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateRouteTableWithRetry(backoffConfig *wait.Backoff, routeTable network.RouteTable) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RouteTablesClient.CreateOrUpdate(az.ResourceGroup, az.RouteTableName, routeTable, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdateRouteWithRetry invokes az.RoutesClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateRouteWithRetry(backoffConfig *wait.Backoff, route network.Route) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RoutesClient.CreateOrUpdate(az.ResourceGroup, az.RouteTableName, *route.Name, route, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// DeleteRouteWithRetry invokes az.RoutesClient.Delete with configurable backoff retry
func (az *Cloud) DeleteRouteWithRetry(backoffConfig *wait.Backoff, routeName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RoutesClient.Delete(az.ResourceGroup, az.RouteTableName, routeName, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// CreateOrUpdateVMWithRetry invokes az.VirtualMachinesClient.CreateOrUpdate with configurable backoff retry
func (az *Cloud) CreateOrUpdateVMWithRetry(backoffConfig *wait.Backoff, vmName string, newVM compute.VirtualMachine) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.VirtualMachinesClient.CreateOrUpdate(az.ResourceGroup, vmName, newVM, nil)
		return processRetryResponse(resp, backoffConfig.Duration, err)
	})
}

// getBackoffConfig delivers a *wait.Backoff configuration from an HTTP response
func (az *Cloud) getBackoffConfig(resp autorest.Response) *wait.Backoff {
	// If the API responds with a "Retry-After" header key/val, we want to respect that
	retryAfter := getRetryAfter(resp)
	if retryAfter > 0 {
		glog.V(2).Infof("backoff: retrying in %d seconds in response to API", retryAfter)
		// Using a linear (factor=1) backoff strategy for API-directed retry intervals
		return &wait.Backoff{
			Duration: retryAfter * time.Second,
			Steps:    retryAfterIterations,
			Factor:   1,
		}
	}
	// If the API has no retry guidance we use the *Cloud default
	if az.CloudProviderBackoff && isRetryResponse(resp.StatusCode) {
		return &az.resourceRequestBackoff
	}
	return nil
}

// A thin wrapper function to determine if an HTTP response code suggests a retry request
func isRetryResponse(statusCode int) bool {
	if statusCode == 429 {
		return true
	}
	return false
}

// Retrieves a 'Retry-After' HTTP header key value, if avail, and converts to time.Duration
func getRetryAfter(resp autorest.Response) time.Duration {
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		waitPeriod, err := strconv.Atoi(retryAfter)
		if err != nil {
			glog.Warning("backoff: got unexpected 'Retry-After' value: %s", retryAfter)
			return 0
		}
		return time.Duration(waitPeriod)
	}
	return 0
}

// A wait.ConditionFunc function to deal with common HTTP backoff response conditions
func processRetryResponse(resp autorest.Response, originalDelay time.Duration, err error) (bool, error) {
	if isSuccessHTTPResponse(resp) {
		glog.V(2).Infof("backoff: success, HTTP response=%d", resp.StatusCode)
		return true, nil
	}
	glog.Errorf("backoff: failure, HTTP response=%d, err=%v", resp.StatusCode, err)

	retryAfter := getRetryAfter(resp)
	// We want to incorporate any additional delay time if the API response suggests our original delay isn't long enough
	// A negative value passed to time.Sleep will return immediately
	time.Sleep(retryAfter - originalDelay)

	// suppress the error object so that backoff process continues
	return false, nil
}

// isSuccessHTTPResponse determines if the response from an HTTP request suggests success
func isSuccessHTTPResponse(resp autorest.Response) bool {
	// HTTP 2xx suggests a successful response
	if 199 < resp.StatusCode && resp.StatusCode < 300 {
		return true
	}
	return false
}
