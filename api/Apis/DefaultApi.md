# DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cancelOperation**](DefaultApi.md#cancelOperation) | **POST** /sessions/{session_handle}/operations/{operation_handle}/cancel | 
[**closeOperation**](DefaultApi.md#closeOperation) | **DELETE** /sessions/{session_handle}/operations/{operation_handle}/close | 
[**closeSession**](DefaultApi.md#closeSession) | **DELETE** /sessions/{session_handle} | 
[**executeStatement**](DefaultApi.md#executeStatement) | **POST** /sessions/{session_handle}/statements | 
[**fetchResults**](DefaultApi.md#fetchResults) | **GET** /sessions/{session_handle}/operations/{operation_handle}/result/{token} | 
[**getApiVersion**](DefaultApi.md#getApiVersion) | **GET** /api_versions | 
[**getInfo**](DefaultApi.md#getInfo) | **GET** /info | 
[**getOperationStatus**](DefaultApi.md#getOperationStatus) | **GET** /sessions/{session_handle}/operations/{operation_handle}/status | 
[**getSessionConfig**](DefaultApi.md#getSessionConfig) | **GET** /sessions/{session_handle} | 
[**openSession**](DefaultApi.md#openSession) | **POST** /sessions | 
[**triggerSession**](DefaultApi.md#triggerSession) | **POST** /sessions/{session_handle}/heartbeat | 


<a name="cancelOperation"></a>
# **cancelOperation**
> OperationStatusResponseBody cancelOperation(session\_handle, operation\_handle)



    Cancel the operation.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**SessionHandle**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]
 **operation\_handle** | [**OperationHandle**](../Models/.md)| The OperationHandle that identifies a operation. | [default to null]

### Return type

[**OperationStatusResponseBody**](../Models/OperationStatusResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="closeOperation"></a>
# **closeOperation**
> OperationStatusResponseBody closeOperation(session\_handle, operation\_handle)



    Close the operation.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]
 **operation\_handle** | [**UUID**](../Models/.md)| The OperationHandle that identifies a operation. | [default to null]

### Return type

[**OperationStatusResponseBody**](../Models/OperationStatusResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="closeSession"></a>
# **closeSession**
> CloseSessionResponseBody closeSession(session\_handle)



    Closes the specific session.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]

### Return type

[**CloseSessionResponseBody**](../Models/CloseSessionResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="executeStatement"></a>
# **executeStatement**
> ExecuteStatementResponseBody executeStatement(session\_handle, ExecuteStatementRequestBody)



    Execute a statement.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]
 **ExecuteStatementRequestBody** | [**ExecuteStatementRequestBody**](../Models/ExecuteStatementRequestBody.md)|  | [optional]

### Return type

[**ExecuteStatementResponseBody**](../Models/ExecuteStatementResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="fetchResults"></a>
# **fetchResults**
> FetchResultsResponseBody fetchResults(session\_handle, operation\_handle, token)



    Fetch results of Operation.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]
 **operation\_handle** | [**UUID**](../Models/.md)| The OperationHandle that identifies a operation. | [default to null]
 **token** | **Long**| The OperationHandle that identifies a operation. | [default to null]

### Return type

[**FetchResultsResponseBody**](../Models/FetchResultsResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getApiVersion"></a>
# **getApiVersion**
> GetApiVersionResponseBody getApiVersion()



    Get the current available versions for the Rest Endpoint. The client can choose one of the return version as the protocol for later communicate.

### Parameters
This endpoint does not need any parameter.

### Return type

[**GetApiVersionResponseBody**](../Models/GetApiVersionResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getInfo"></a>
# **getInfo**
> GetInfoResponseBody getInfo()



    Get meta data for this cluster.

### Parameters
This endpoint does not need any parameter.

### Return type

[**GetInfoResponseBody**](../Models/GetInfoResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getOperationStatus"></a>
# **getOperationStatus**
> OperationStatusResponseBody getOperationStatus(session\_handle, operation\_handle)



    Get the status of operation.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]
 **operation\_handle** | [**UUID**](../Models/.md)| The OperationHandle that identifies a operation. | [default to null]

### Return type

[**OperationStatusResponseBody**](../Models/OperationStatusResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getSessionConfig"></a>
# **getSessionConfig**
> GetSessionConfigResponseBody getSessionConfig(session\_handle)



    Get the session configuration.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**UUID**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]

### Return type

[**GetSessionConfigResponseBody**](../Models/GetSessionConfigResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="openSession"></a>
# **openSession**
> OpenSessionResponseBody openSession(OpenSessionRequestBody)



    Opens a new session with specific properties. Specific properties can be given for current session which will override the default properties of gateway.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **OpenSessionRequestBody** | [**OpenSessionRequestBody**](../Models/OpenSessionRequestBody.md)|  | [optional]

### Return type

[**OpenSessionResponseBody**](../Models/OpenSessionResponseBody.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="triggerSession"></a>
# **triggerSession**
> triggerSession(session\_handle)



    Trigger heartbeat to tell the server that the client is active, and to keep the session alive as long as configured timeout value.

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **session\_handle** | [**SessionHandle**](../Models/.md)| The SessionHandle that identifies a session. | [default to null]

### Return type

null (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

