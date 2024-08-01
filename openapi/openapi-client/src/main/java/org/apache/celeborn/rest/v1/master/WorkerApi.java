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

import org.apache.celeborn.rest.v1.model.ExcludeWorkerRequest;
import org.apache.celeborn.rest.v1.model.HandleResponse;
import org.apache.celeborn.rest.v1.model.RemoveWorkersUnavailableInfoRequest;
import org.apache.celeborn.rest.v1.model.SendWorkerEventRequest;
import org.apache.celeborn.rest.v1.model.WorkerEventsResponse;
import org.apache.celeborn.rest.v1.model.WorkersResponse;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.7.0")
public class WorkerApi extends BaseApi {

  public WorkerApi() {
    super(Configuration.getDefaultApiClient());
  }

  public WorkerApi(ApiClient apiClient) {
    super(apiClient);
  }

  /**
   * 
   * Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove. 
   * @param excludeWorkerRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse excludeWorker(ExcludeWorkerRequest excludeWorkerRequest) throws ApiException {
    return this.excludeWorker(excludeWorkerRequest, Collections.emptyMap());
  }


  /**
   * 
   * Excluded workers of the master add or remove the worker manually given worker id. The parameter add or remove specifies the excluded workers to add or remove. 
   * @param excludeWorkerRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse excludeWorker(ExcludeWorkerRequest excludeWorkerRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = excludeWorkerRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/exclude";

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
   * List all worker event infos of the master.
   * @return WorkerEventsResponse
   * @throws ApiException if fails to make API call
   */
  public WorkerEventsResponse getWorkerEvents() throws ApiException {
    return this.getWorkerEvents(Collections.emptyMap());
  }


  /**
   * 
   * List all worker event infos of the master.
   * @param additionalHeaders additionalHeaders for this call
   * @return WorkerEventsResponse
   * @throws ApiException if fails to make API call
   */
  public WorkerEventsResponse getWorkerEvents(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/events";

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

    TypeReference<WorkerEventsResponse> localVarReturnType = new TypeReference<WorkerEventsResponse>() {};
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
   * List worker information of the service. It will list all registered workers, lost workers, excluded workers, shutdown workers and decommission workers information. 
   * @return WorkersResponse
   * @throws ApiException if fails to make API call
   */
  public WorkersResponse getWorkers() throws ApiException {
    return this.getWorkers(Collections.emptyMap());
  }


  /**
   * 
   * List worker information of the service. It will list all registered workers, lost workers, excluded workers, shutdown workers and decommission workers information. 
   * @param additionalHeaders additionalHeaders for this call
   * @return WorkersResponse
   * @throws ApiException if fails to make API call
   */
  public WorkersResponse getWorkers(Map<String, String> additionalHeaders) throws ApiException {
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

    TypeReference<WorkersResponse> localVarReturnType = new TypeReference<WorkersResponse>() {};
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
   * Remove the unavailable workers info from the master.
   * @param removeWorkersUnavailableInfoRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse removeWorkersUnavailableInfo(RemoveWorkersUnavailableInfoRequest removeWorkersUnavailableInfoRequest) throws ApiException {
    return this.removeWorkersUnavailableInfo(removeWorkersUnavailableInfoRequest, Collections.emptyMap());
  }


  /**
   * 
   * Remove the unavailable workers info from the master.
   * @param removeWorkersUnavailableInfoRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse removeWorkersUnavailableInfo(RemoveWorkersUnavailableInfoRequest removeWorkersUnavailableInfoRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = removeWorkersUnavailableInfoRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/unavailable";

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
        "DELETE",
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
   * For Master(Leader) can send worker event to manager workers. Legal types are &#39;None&#39;, &#39;Immediately&#39;, &#39;Decommission&#39;, &#39;DecommissionThenIdle&#39;, &#39;Graceful&#39;, &#39;Recommission&#39;. 
   * @param sendWorkerEventRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse sendWorkerEvent(SendWorkerEventRequest sendWorkerEventRequest) throws ApiException {
    return this.sendWorkerEvent(sendWorkerEventRequest, Collections.emptyMap());
  }


  /**
   * 
   * For Master(Leader) can send worker event to manager workers. Legal types are &#39;None&#39;, &#39;Immediately&#39;, &#39;Decommission&#39;, &#39;DecommissionThenIdle&#39;, &#39;Graceful&#39;, &#39;Recommission&#39;. 
   * @param sendWorkerEventRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse sendWorkerEvent(SendWorkerEventRequest sendWorkerEventRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = sendWorkerEventRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/workers/events";

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
