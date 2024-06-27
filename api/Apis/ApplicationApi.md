# ApplicationApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getApplicationHostNames**](ApplicationApi.md#getApplicationHostNames) | **GET** /api/v1/applications/hostnames |  |
| [**getApplications**](ApplicationApi.md#getApplications) | **GET** /api/v1/applications |  |
| [**getApplicationsDiskUsage**](ApplicationApi.md#getApplicationsDiskUsage) | **GET** /api/v1/applications/top_disk_usages |  |


<a name="getApplicationHostNames"></a>
# **getApplicationHostNames**
> ApplicationHostnameResponse getApplicationHostNames()



    List the hostnames of the applications.

### Parameters
This endpoint does not need any parameter.

### Return type

[**ApplicationHostnameResponse**](../Models/ApplicationHostnameResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getApplications"></a>
# **getApplications**
> ApplicationsInfoResponse getApplications()



    List the application information.

### Parameters
This endpoint does not need any parameter.

### Return type

[**ApplicationsInfoResponse**](../Models/ApplicationsInfoResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getApplicationsDiskUsage"></a>
# **getApplicationsDiskUsage**
> AppDiskUsageSnapshotResponse getApplicationsDiskUsage()



    List the application disk usage.

### Parameters
This endpoint does not need any parameter.

### Return type

[**AppDiskUsageSnapshotResponse**](../Models/AppDiskUsageSnapshotResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

