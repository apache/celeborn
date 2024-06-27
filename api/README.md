# Documentation for Celeborn Master REST API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *ConfApi* | [**getConf**](Apis/ConfApi.md#getconf) | **GET** /api/v1/conf | List the conf setting. |
*ConfApi* | [**getDynamicConf**](Apis/ConfApi.md#getdynamicconf) | **GET** /api/v1/conf/dynamic | List the dynamic configs. The parameter level specifies the config level of dynamic configs.  The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level.  |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [ConfResponse](./Models/ConfResponse.md)
 - [Config](./Models/Config.md)
 - [DynamicConfig](./Models/DynamicConfig.md)
 - [ThreadStack](./Models/ThreadStack.md)
 - [ThreadStackResponse](./Models/ThreadStackResponse.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
