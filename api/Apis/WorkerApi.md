# WorkerApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**excludeWorker**](WorkerApi.md#excludeWorker) | **POST** /api/v1/workers/exclude |  |
| [**getWorkerEvents**](WorkerApi.md#getWorkerEvents) | **GET** /api/v1/workers/events |  |
| [**getWorkers**](WorkerApi.md#getWorkers) | **GET** /api/v1/workers |  |
| [**sendWorkerEvent**](WorkerApi.md#sendWorkerEvent) | **POST** /api/v1/workers/events |  |


<a name="excludeWorker"></a>
# **excludeWorker**
> ExcludeWorkerResponse excludeWorker(ExcludeWorkerRequest)



    Exclude worker.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **ExcludeWorkerRequest** | [**ExcludeWorkerRequest**](../Models/ExcludeWorkerRequest.md)|  | [optional] |

### Return type

[**ExcludeWorkerResponse**](../Models/ExcludeWorkerResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="getWorkerEvents"></a>
# **getWorkerEvents**
> WorkerEvent getWorkerEvents()



    List the worker events.

### Parameters
This endpoint does not need any parameter.

### Return type

[**WorkerEvent**](../Models/WorkerEvent.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getWorkers"></a>
# **getWorkers**
> WorkersResponse getWorkers()



    List the worker information.

### Parameters
This endpoint does not need any parameter.

### Return type

[**WorkersResponse**](../Models/WorkersResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="sendWorkerEvent"></a>
# **sendWorkerEvent**
> SendWorkerEventResponse sendWorkerEvent(SendWorkerEventRequest)



    Send worker event.

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **SendWorkerEventRequest** | [**SendWorkerEventRequest**](../Models/SendWorkerEventRequest.md)|  | [optional] |

### Return type

[**SendWorkerEventResponse**](../Models/SendWorkerEventResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

