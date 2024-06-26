# Documentation for Flink SQL Gateway REST API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DefaultApi* | [**cancelOperation**](Apis/DefaultApi.md#canceloperation) | **POST** /sessions/{session_handle}/operations/{operation_handle}/cancel | Cancel the operation.
*DefaultApi* | [**closeOperation**](Apis/DefaultApi.md#closeoperation) | **DELETE** /sessions/{session_handle}/operations/{operation_handle}/close | Close the operation.
*DefaultApi* | [**closeSession**](Apis/DefaultApi.md#closesession) | **DELETE** /sessions/{session_handle} | Closes the specific session.
*DefaultApi* | [**executeStatement**](Apis/DefaultApi.md#executestatement) | **POST** /sessions/{session_handle}/statements | Execute a statement.
*DefaultApi* | [**fetchResults**](Apis/DefaultApi.md#fetchresults) | **GET** /sessions/{session_handle}/operations/{operation_handle}/result/{token} | Fetch results of Operation.
*DefaultApi* | [**getApiVersion**](Apis/DefaultApi.md#getapiversion) | **GET** /api_versions | Get the current available versions for the Rest Endpoint. The client can choose one of the return version as the protocol for later communicate.
*DefaultApi* | [**getInfo**](Apis/DefaultApi.md#getinfo) | **GET** /info | Get meta data for this cluster.
*DefaultApi* | [**getOperationStatus**](Apis/DefaultApi.md#getoperationstatus) | **GET** /sessions/{session_handle}/operations/{operation_handle}/status | Get the status of operation.
*DefaultApi* | [**getSessionConfig**](Apis/DefaultApi.md#getsessionconfig) | **GET** /sessions/{session_handle} | Get the session configuration.
*DefaultApi* | [**openSession**](Apis/DefaultApi.md#opensession) | **POST** /sessions | Opens a new session with specific properties. Specific properties can be given for current session which will override the default properties of gateway.
*DefaultApi* | [**triggerSession**](Apis/DefaultApi.md#triggersession) | **POST** /sessions/{session_handle}/heartbeat | Trigger heartbeat to tell the server that the client is active, and to keep the session alive as long as configured timeout value.


<a name="documentation-for-models"></a>
## Documentation for Models

 - [CloseSessionResponseBody](./Models/CloseSessionResponseBody.md)
 - [Column](./Models/Column.md)
 - [ConstraintType](./Models/ConstraintType.md)
 - [DataType](./Models/DataType.md)
 - [ExecuteStatementRequestBody](./Models/ExecuteStatementRequestBody.md)
 - [ExecuteStatementResponseBody](./Models/ExecuteStatementResponseBody.md)
 - [FetchResultsResponseBody](./Models/FetchResultsResponseBody.md)
 - [GetApiVersionResponseBody](./Models/GetApiVersionResponseBody.md)
 - [GetInfoResponseBody](./Models/GetInfoResponseBody.md)
 - [GetSessionConfigResponseBody](./Models/GetSessionConfigResponseBody.md)
 - [LogicalType](./Models/LogicalType.md)
 - [LogicalTypeRoot](./Models/LogicalTypeRoot.md)
 - [OpenSessionRequestBody](./Models/OpenSessionRequestBody.md)
 - [OpenSessionResponseBody](./Models/OpenSessionResponseBody.md)
 - [OperationHandle](./Models/OperationHandle.md)
 - [OperationStatusResponseBody](./Models/OperationStatusResponseBody.md)
 - [ResolvedExpression](./Models/ResolvedExpression.md)
 - [ResolvedSchema](./Models/ResolvedSchema.md)
 - [ResultSet](./Models/ResultSet.md)
 - [ResultSetData](./Models/ResultSetData.md)
 - [ResultType](./Models/ResultType.md)
 - [RowData](./Models/RowData.md)
 - [RowKind](./Models/RowKind.md)
 - [SerializedThrowable](./Models/SerializedThrowable.md)
 - [SessionHandle](./Models/SessionHandle.md)
 - [UniqueConstraint](./Models/UniqueConstraint.md)
 - [WatermarkSpec](./Models/WatermarkSpec.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

<a name="basicAuth"></a>
### basicAuth

- **Type**: HTTP basic authentication

