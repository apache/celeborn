/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.rest.v1.worker;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.celeborn.rest.v1.worker.invoker.ApiException;
import org.apache.celeborn.rest.v1.worker.invoker.ApiClient;
import org.apache.celeborn.rest.v1.worker.invoker.BaseApi;
import org.apache.celeborn.rest.v1.worker.invoker.Configuration;
import org.apache.celeborn.rest.v1.worker.invoker.Pair;

import org.apache.celeborn.rest.v1.model.ConfResponse;
import org.apache.celeborn.rest.v1.model.DynamicConfigResponse;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class ConfApi extends BaseApi {

  public ConfApi() {
    super(Configuration.getDefaultApiClient());
  }

  public ConfApi(ApiClient apiClient) {
    super(apiClient);
  }

  /**
   * 
   * List the conf setting.
   * @return ConfResponse
   * @throws ApiException if fails to make API call
   */
  public ConfResponse getConf() throws ApiException {
    return this.getConf(Collections.emptyMap());
  }


  /**
   * 
   * List the conf setting.
   * @param additionalHeaders additionalHeaders for this call
   * @return ConfResponse
   * @throws ApiException if fails to make API call
   */
  public ConfResponse getConf(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/conf";

    StringJoiner localVarQueryStringJoiner = new StringJoiner("&");
    String localVarQueryParameterBaseName;
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, String> localVarCookieParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    
    localVarHeaderParams.putAll(additionalHeaders);

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    TypeReference<ConfResponse> localVarReturnType = new TypeReference<ConfResponse>() {};
    return apiClient.invokeAPI(
        localVarPath,
        "GET",
        localVarQueryParams,
        localVarCollectionQueryParams,
        localVarQueryStringJoiner.toString(),
        localVarPostBody,
        localVarHeaderParams,
        localVarCookieParams,
        localVarFormParams,
        localVarAccept,
        localVarContentType,
        localVarAuthNames,
        localVarReturnType
    );
  }

  /**
   * 
   * List the dynamic configs. The parameter level specifies the config level of dynamic configs.  The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level. 
   * @param level the config level of dynamic configs. (optional)
   * @param tenant the tenant id of TENANT or TENANT_USER level. (optional)
   * @param name the user name of TENANT_USER level. (optional)
   * @return DynamicConfigResponse
   * @throws ApiException if fails to make API call
   */
  public DynamicConfigResponse getDynamicConf(@javax.annotation.Nullable String level, @javax.annotation.Nullable String tenant, @javax.annotation.Nullable String name) throws ApiException {
    return this.getDynamicConf(level, tenant, name, Collections.emptyMap());
  }


  /**
   * 
   * List the dynamic configs. The parameter level specifies the config level of dynamic configs.  The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. The parameter name specifies the user name of TENANT_USER level. Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level. 
   * @param level the config level of dynamic configs. (optional)
   * @param tenant the tenant id of TENANT or TENANT_USER level. (optional)
   * @param name the user name of TENANT_USER level. (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return DynamicConfigResponse
   * @throws ApiException if fails to make API call
   */
  public DynamicConfigResponse getDynamicConf(@javax.annotation.Nullable String level, @javax.annotation.Nullable String tenant, @javax.annotation.Nullable String name, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/conf/dynamic";

    StringJoiner localVarQueryStringJoiner = new StringJoiner("&");
    String localVarQueryParameterBaseName;
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, String> localVarCookieParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarQueryParams.addAll(apiClient.parameterToPair("level", level));
    localVarQueryParams.addAll(apiClient.parameterToPair("tenant", tenant));
    localVarQueryParams.addAll(apiClient.parameterToPair("name", name));
    
    localVarHeaderParams.putAll(additionalHeaders);

    
    
    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    TypeReference<DynamicConfigResponse> localVarReturnType = new TypeReference<DynamicConfigResponse>() {};
    return apiClient.invokeAPI(
        localVarPath,
        "GET",
        localVarQueryParams,
        localVarCollectionQueryParams,
        localVarQueryStringJoiner.toString(),
        localVarPostBody,
        localVarHeaderParams,
        localVarCookieParams,
        localVarFormParams,
        localVarAccept,
        localVarContentType,
        localVarAuthNames,
        localVarReturnType
    );
  }

  @Override
  public <T> T invokeAPI(String url, String method, Object request, TypeReference<T> returnType, Map<String, String> additionalHeaders) throws ApiException {
    String localVarPath = url.replace(apiClient.getBaseURL(), "");
    StringJoiner localVarQueryStringJoiner = new StringJoiner("&");
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, String> localVarCookieParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    localVarHeaderParams.putAll(additionalHeaders);

    final String[] localVarAccepts = {
      "application/json"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    return apiClient.invokeAPI(
      localVarPath,
        method,
        localVarQueryParams,
        localVarCollectionQueryParams,
        localVarQueryStringJoiner.toString(),
        request,
        localVarHeaderParams,
        localVarCookieParams,
        localVarFormParams,
        localVarAccept,
        localVarContentType,
        localVarAuthNames,
        returnType
    );
  }
}
