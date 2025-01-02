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

import org.apache.celeborn.rest.v1.model.HandleResponse;
import org.apache.celeborn.rest.v1.model.UnAvailablePeersResponse;
import org.apache.celeborn.rest.v1.model.WorkerExitRequest;
import org.apache.celeborn.rest.v1.model.WorkerInfoResponse;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class WorkerApi extends BaseApi {

  public WorkerApi() {
    super(Configuration.getDefaultApiClient());
  }

  public WorkerApi(ApiClient apiClient) {
    super(apiClient);
  }

  /**
   * 
   * List the worker information.
   * @return WorkerInfoResponse
   * @throws ApiException if fails to make API call
   */
  public WorkerInfoResponse getWorkerInfo() throws ApiException {
    return this.getWorkerInfo(Collections.emptyMap());
  }


  /**
   * 
   * List the worker information.
   * @param additionalHeaders additionalHeaders for this call
   * @return WorkerInfoResponse
   * @throws ApiException if fails to make API call
   */
  public WorkerInfoResponse getWorkerInfo(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers";

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

    TypeReference<WorkerInfoResponse> localVarReturnType = new TypeReference<WorkerInfoResponse>() {};
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
   * List the unavailable peers of the worker, this always means the worker connect to the peer failed. 
   * @return UnAvailablePeersResponse
   * @throws ApiException if fails to make API call
   */
  public UnAvailablePeersResponse unavailablePeers() throws ApiException {
    return this.unavailablePeers(Collections.emptyMap());
  }


  /**
   * 
   * List the unavailable peers of the worker, this always means the worker connect to the peer failed. 
   * @param additionalHeaders additionalHeaders for this call
   * @return UnAvailablePeersResponse
   * @throws ApiException if fails to make API call
   */
  public UnAvailablePeersResponse unavailablePeers(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/unavailable_peers";

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

    TypeReference<UnAvailablePeersResponse> localVarReturnType = new TypeReference<UnAvailablePeersResponse>() {};
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
   * Trigger this worker to exit. Legal exit types are &#39;DECOMMISSION&#39;, &#39;GRACEFUL&#39; and &#39;IMMEDIATELY&#39;. 
   * @param workerExitRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse workerExit(WorkerExitRequest workerExitRequest) throws ApiException {
    return this.workerExit(workerExitRequest, Collections.emptyMap());
  }


  /**
   * 
   * Trigger this worker to exit. Legal exit types are &#39;DECOMMISSION&#39;, &#39;GRACEFUL&#39; and &#39;IMMEDIATELY&#39;. 
   * @param workerExitRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse workerExit(WorkerExitRequest workerExitRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = workerExitRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/exit";

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
