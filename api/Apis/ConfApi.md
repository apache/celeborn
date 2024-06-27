# ConfApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getConf**](ConfApi.md#getConf) | **GET** /api/v1/conf |  |
| [**getDynamicConf**](ConfApi.md#getDynamicConf) | **GET** /api/v1/conf/dynamic |  |


<a name="getConf"></a>
# **getConf**
> ConfResponse getConf()



    List the conf setting.

### Parameters
This endpoint does not need any parameter.

### Return type

[**ConfResponse**](../Models/ConfResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getDynamicConf"></a>
# **getDynamicConf**
> DynamicConfig getDynamicConf(level, tenant, name)



    List the dynamic configs. The parameter level specifies the config level of dynamic configs.  The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **level** | **String**| the config level of dynamic configs. | [optional] [default to null] |
| **tenant** | **String**| the tenant id of TENANT or TENANT_USER level. | [optional] [default to null] |
| **name** | **String**| the user name of TENANT_USER level. | [optional] [default to null] |

### Return type

[**DynamicConfig**](../Models/DynamicConfig.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

