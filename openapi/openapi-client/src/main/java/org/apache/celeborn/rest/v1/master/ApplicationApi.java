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

package org.apache.celeborn.rest.v1.master;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.celeborn.rest.v1.master.invoker.ApiException;
import org.apache.celeborn.rest.v1.master.invoker.ApiClient;
import org.apache.celeborn.rest.v1.master.invoker.BaseApi;
import org.apache.celeborn.rest.v1.master.invoker.Configuration;
import org.apache.celeborn.rest.v1.master.invoker.Pair;

import org.apache.celeborn.rest.v1.model.ApplicationsHeartbeatResponse;
import org.apache.celeborn.rest.v1.model.DeleteAppsRequest;
import org.apache.celeborn.rest.v1.model.HandleResponse;
import org.apache.celeborn.rest.v1.model.HostnamesResponse;
import org.apache.celeborn.rest.v1.model.ReviseLostShufflesRequest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class ApplicationApi extends BaseApi {

  public ApplicationApi() {
    super(Configuration.getDefaultApiClient());
  }

  public ApplicationApi(ApiClient apiClient) {
    super(apiClient);
  }

  /**
   * 
   * Delete resource of apps.
   * @param deleteAppsRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse deleteApps(DeleteAppsRequest deleteAppsRequest) throws ApiException {
    return this.deleteApps(deleteAppsRequest, Collections.emptyMap());
  }


  /**
   * 
   * Delete resource of apps.
   * @param deleteAppsRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse deleteApps(DeleteAppsRequest deleteAppsRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = deleteAppsRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/applications/delete_apps";

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
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    TypeReference<HandleResponse> localVarReturnType = new TypeReference<HandleResponse>() {};
    return apiClient.invokeAPI(
        localVarPath,
        "POST",
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
   * List all running application&#39;s LifecycleManager&#39;s hostnames of the cluster.
   * @return HostnamesResponse
   * @throws ApiException if fails to make API call
   */
  public HostnamesResponse getApplicationHostNames() throws ApiException {
    return this.getApplicationHostNames(Collections.emptyMap());
  }


  /**
   * 
   * List all running application&#39;s LifecycleManager&#39;s hostnames of the cluster.
   * @param additionalHeaders additionalHeaders for this call
   * @return HostnamesResponse
   * @throws ApiException if fails to make API call
   */
  public HostnamesResponse getApplicationHostNames(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/applications/hostnames";

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

    TypeReference<HostnamesResponse> localVarReturnType = new TypeReference<HostnamesResponse>() {};
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
   * List all running application&#39;s ids of the cluster.
   * @return ApplicationsHeartbeatResponse
   * @throws ApiException if fails to make API call
   */
  public ApplicationsHeartbeatResponse getApplications() throws ApiException {
    return this.getApplications(Collections.emptyMap());
  }


  /**
   * 
   * List all running application&#39;s ids of the cluster.
   * @param additionalHeaders additionalHeaders for this call
   * @return ApplicationsHeartbeatResponse
   * @throws ApiException if fails to make API call
   */
  public ApplicationsHeartbeatResponse getApplications(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/applications";

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

    TypeReference<ApplicationsHeartbeatResponse> localVarReturnType = new TypeReference<ApplicationsHeartbeatResponse>() {};
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
   * Revise lost shuffles or deleted shuffles of an application.
   * @param reviseLostShufflesRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse reviseLostShuffles(ReviseLostShufflesRequest reviseLostShufflesRequest) throws ApiException {
    return this.reviseLostShuffles(reviseLostShufflesRequest, Collections.emptyMap());
  }


  /**
   * 
   * Revise lost shuffles or deleted shuffles of an application.
   * @param reviseLostShufflesRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse reviseLostShuffles(ReviseLostShufflesRequest reviseLostShufflesRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = reviseLostShufflesRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/applications/revise_lost_shuffles";

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
      "application/json"
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    TypeReference<HandleResponse> localVarReturnType = new TypeReference<HandleResponse>() {};
    return apiClient.invokeAPI(
        localVarPath,
        "POST",
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
      "application/json"
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
