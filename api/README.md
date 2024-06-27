# Documentation for Celeborn Master REST API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *ApplicationApi* | [**getApplicationHostNames**](Apis/ApplicationApi.md#getapplicationhostnames) | **GET** /api/v1/applications/hostnames | List the hostnames of the applications. |
*ApplicationApi* | [**getApplications**](Apis/ApplicationApi.md#getapplications) | **GET** /api/v1/applications | List the application information. |
*ApplicationApi* | [**getApplicationsDiskUsage**](Apis/ApplicationApi.md#getapplicationsdiskusage) | **GET** /api/v1/applications/top_disk_usages | List the application disk usage. |
| *ConfApi* | [**getConf**](Apis/ConfApi.md#getconf) | **GET** /api/v1/conf | List the conf setting. |
*ConfApi* | [**getDynamicConf**](Apis/ConfApi.md#getdynamicconf) | **GET** /api/v1/conf/dynamic | List the dynamic configs. The parameter level specifies the config level of dynamic configs.  The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level.  |
| *DefaultApi* | [**getThreadDump**](Apis/DefaultApi.md#getthreaddump) | **GET** /api/v1/thread_dump | List the thread dump. |
| *MasterApi* | [**getMasterGroupInfo**](Apis/MasterApi.md#getmastergroupinfo) | **GET** /api/v1/masters | List the master group information. |
| *ShuffleApi* | [**getShuffles**](Apis/ShuffleApi.md#getshuffles) | **GET** /api/v1/shuffles | List the shuffle information. |
| *WorkerApi* | [**excludeWorker**](Apis/WorkerApi.md#excludeworker) | **POST** /api/v1/workers/exclude | Exclude worker. |
*WorkerApi* | [**getWorkerEvents**](Apis/WorkerApi.md#getworkerevents) | **GET** /api/v1/workers/events | List the worker events. |
*WorkerApi* | [**getWorkers**](Apis/WorkerApi.md#getworkers) | **GET** /api/v1/workers | List the worker information. |
*WorkerApi* | [**sendWorkerEvent**](Apis/WorkerApi.md#sendworkerevent) | **POST** /api/v1/workers/events | Send worker event. |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AppDiskUsage](./Models/AppDiskUsage.md)
 - [AppDiskUsageSnapshotResponse](./Models/AppDiskUsageSnapshotResponse.md)
 - [ApplicationHeartbeat](./Models/ApplicationHeartbeat.md)
 - [ApplicationHostnameResponse](./Models/ApplicationHostnameResponse.md)
 - [ApplicationsInfoResponse](./Models/ApplicationsInfoResponse.md)
 - [ConfResponse](./Models/ConfResponse.md)
 - [Config](./Models/Config.md)
 - [DynamicConfigResponse](./Models/DynamicConfigResponse.md)
 - [ExcludeWorkerRequest](./Models/ExcludeWorkerRequest.md)
 - [ExcludeWorkerResponse](./Models/ExcludeWorkerResponse.md)
 - [MasterCommitInfo](./Models/MasterCommitInfo.md)
 - [MasterInfoResponse](./Models/MasterInfoResponse.md)
 - [MasterLeader](./Models/MasterLeader.md)
 - [PartitionLocationData](./Models/PartitionLocationData.md)
 - [SendWorkerEventRequest](./Models/SendWorkerEventRequest.md)
 - [SendWorkerEventResponse](./Models/SendWorkerEventResponse.md)
 - [ShuffleResponse](./Models/ShuffleResponse.md)
 - [ThreadStack](./Models/ThreadStack.md)
 - [ThreadStackResponse](./Models/ThreadStackResponse.md)
 - [WorkerEvent](./Models/WorkerEvent.md)
 - [WorkerEventInfo](./Models/WorkerEventInfo.md)
 - [WorkerExitRequest](./Models/WorkerExitRequest.md)
 - [WorkerExitResponse](./Models/WorkerExitResponse.md)
 - [WorkerId](./Models/WorkerId.md)
 - [WorkerInfo](./Models/WorkerInfo.md)
 - [WorkersResponse](./Models/WorkersResponse.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
