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

// If we get a retry-after in HTTP response header, how many times to retry using that interval
const retryAfterRetries = 5

// GetVirtualMachineWithRetry invokes az.getVirtualMachine with exponential backoff retry
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

// CreateOrUpdateSGWithRetry invokes az.SecurityGroupsClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateSGWithRetry(backoffConfig *wait.Backoff, sg network.SecurityGroup) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.SecurityGroupsClient.CreateOrUpdate(az.ResourceGroup, *sg.Name, sg, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdateLBWithRetry invokes az.LoadBalancerClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateLBWithRetry(backoffConfig *wait.Backoff, lb network.LoadBalancer) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.LoadBalancerClient.CreateOrUpdate(az.ResourceGroup, *lb.Name, lb, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdatePIPWithRetry invokes az.PublicIPAddressesClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdatePIPWithRetry(backoffConfig *wait.Backoff, pip network.PublicIPAddress) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.PublicIPAddressesClient.CreateOrUpdate(az.ResourceGroup, *pip.Name, pip, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdateInterfaceWithRetry invokes az.PublicIPAddressesClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateInterfaceWithRetry(backoffConfig *wait.Backoff, nic network.Interface) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.InterfacesClient.CreateOrUpdate(az.ResourceGroup, *nic.Name, nic, nil)
		return processRetryResponse(resp, err)
	})
}

// DeletePublicIPWithRetry invokes az.PublicIPAddressesClient.Delete with exponential backoff retry
func (az *Cloud) DeletePublicIPWithRetry(backoffConfig *wait.Backoff, pipName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.PublicIPAddressesClient.Delete(az.ResourceGroup, pipName, nil)
		return processRetryResponse(resp, err)
	})
}

// DeleteLBWithRetry invokes az.LoadBalancerClient.Delete with exponential backoff retry
func (az *Cloud) DeleteLBWithRetry(backoffConfig *wait.Backoff, lbName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.LoadBalancerClient.Delete(az.ResourceGroup, lbName, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdateRouteTableWithRetry invokes az.RouteTablesClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateRouteTableWithRetry(backoffConfig *wait.Backoff, routeTable network.RouteTable) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RouteTablesClient.CreateOrUpdate(az.ResourceGroup, az.RouteTableName, routeTable, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdateRouteWithRetry invokes az.RoutesClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateRouteWithRetry(backoffConfig *wait.Backoff, route network.Route) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RoutesClient.CreateOrUpdate(az.ResourceGroup, az.RouteTableName, *route.Name, route, nil)
		return processRetryResponse(resp, err)
	})
}

// DeleteRouteWithRetry invokes az.RoutesClient.Delete with exponential backoff retry
func (az *Cloud) DeleteRouteWithRetry(backoffConfig *wait.Backoff, routeName string) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.RoutesClient.Delete(az.ResourceGroup, az.RouteTableName, routeName, nil)
		return processRetryResponse(resp, err)
	})
}

// CreateOrUpdateVMWithRetry invokes az.VirtualMachinesClient.CreateOrUpdate with exponential backoff retry
func (az *Cloud) CreateOrUpdateVMWithRetry(backoffConfig *wait.Backoff, vmName string, newVM compute.VirtualMachine) error {
	return wait.ExponentialBackoff(*backoffConfig, func() (bool, error) {
		az.operationPollRateLimiter.Accept()
		resp, err := az.VirtualMachinesClient.CreateOrUpdate(az.ResourceGroup, vmName, newVM, nil)
		return processRetryResponse(resp, err)
	})
}

// getBackoffConfig delivers a *wait.Backoff configuration from an HTTP response
func (az *Cloud) getBackoffConfig(resp autorest.Response) *wait.Backoff {
	// If the API responds with a "Retry-After" header key/val, we want to respect that
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		waitPeriod, err := strconv.Atoi(retryAfter)
		if err == nil {
			glog.V(2).Infof("backoff: retrying in %d seconds in response to API", waitPeriod)
			return &wait.Backoff{
				Duration: time.Duration(waitPeriod) * time.Second,
				Steps:    retryAfterRetries,
			}
		}
		glog.Warning("backoff: got unexpected 'Retry-After' value: %s", retryAfter)
	}
	// We should retry HTTP 429 responses if backoff is turned on
	if az.CloudProviderBackoff && isRetryResponse(resp.StatusCode) {
		return &wait.Backoff{
			Steps:    az.CloudProviderBackoffRetries,
			Factor:   az.CloudProviderBackoffExponent,
			Duration: time.Duration(az.CloudProviderBackoffDuration) * time.Second,
			Jitter:   az.CloudProviderBackoffJitter,
		}
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

// A wait.ConditionFunc function to deal with common HTTP backoff response conditions
func processRetryResponse(resp autorest.Response, err error) (bool, error) {
	if isSuccessHTTPResponse(resp) {
		glog.V(2).Infof("backoff: success, HTTP response=%d", resp.StatusCode)
		return true, nil
	}
	glog.Errorf("backoff: failure, HTTP response=%d, err=%v", resp.StatusCode, err)
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
