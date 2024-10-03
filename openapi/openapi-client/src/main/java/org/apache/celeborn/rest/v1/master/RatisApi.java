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

import java.io.File;
import org.apache.celeborn.rest.v1.model.HandleResponse;
import org.apache.celeborn.rest.v1.model.RatisElectionTransferRequest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class RatisApi extends BaseApi {

  public RatisApi() {
    super(Configuration.getDefaultApiClient());
  }

  public RatisApi(ApiClient apiClient) {
    super(apiClient);
  }

  /**
   * 
   * Trigger the current server take snapshot.
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse createRatisSnapshot() throws ApiException {
    return this.createRatisSnapshot(Collections.emptyMap());
  }


  /**
   * 
   * Trigger the current server take snapshot.
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse createRatisSnapshot(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/snapshot/create";

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
   * Generate a new-raft-meta.conf file.
   * @return File
   * @throws ApiException if fails to make API call
   */
  public File localRaftMetaConf() throws ApiException {
    return this.localRaftMetaConf(Collections.emptyMap());
  }


  /**
   * 
   * Generate a new-raft-meta.conf file.
   * @param additionalHeaders additionalHeaders for this call
   * @return File
   * @throws ApiException if fails to make API call
   */
  public File localRaftMetaConf(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/local/raft_meta_conf";

    StringJoiner localVarQueryStringJoiner = new StringJoiner("&");
    String localVarQueryParameterBaseName;
    List<Pair> localVarQueryParams = new ArrayList<Pair>();
    List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
    Map<String, String> localVarHeaderParams = new HashMap<String, String>();
    Map<String, String> localVarCookieParams = new HashMap<String, String>();
    Map<String, Object> localVarFormParams = new HashMap<String, Object>();

    
    localVarHeaderParams.putAll(additionalHeaders);

    
    
    final String[] localVarAccepts = {
      "application/octet-stream"
    };
    final String localVarAccept = apiClient.selectHeaderAccept(localVarAccepts);

    final String[] localVarContentTypes = {
      
    };
    final String localVarContentType = apiClient.selectHeaderContentType(localVarContentTypes);

    String[] localVarAuthNames = new String[] { "basic" };

    TypeReference<File> localVarReturnType = new TypeReference<File>() {};
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
   * Pause leader election at the current server. Then, the current server would not start a leader election.
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse pauseRatisElection() throws ApiException {
    return this.pauseRatisElection(Collections.emptyMap());
  }


  /**
   * 
   * Pause leader election at the current server. Then, the current server would not start a leader election.
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse pauseRatisElection(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/election/pause";

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
   * Resume leader election at the current server.
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse resumeRatisElection() throws ApiException {
    return this.resumeRatisElection(Collections.emptyMap());
  }


  /**
   * 
   * Resume leader election at the current server.
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse resumeRatisElection(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/election/resume";

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
   * Make the group leader step down its leadership.
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse stepdownRatisLeader() throws ApiException {
    return this.stepdownRatisLeader(Collections.emptyMap());
  }


  /**
   * 
   * Make the group leader step down its leadership.
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse stepdownRatisLeader(Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = null;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/election/stepdown";

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
   * Transfer the group leader to the specified server.
   * @param ratisElectionTransferRequest  (optional)
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse transferRatisLeader(RatisElectionTransferRequest ratisElectionTransferRequest) throws ApiException {
    return this.transferRatisLeader(ratisElectionTransferRequest, Collections.emptyMap());
  }


  /**
   * 
   * Transfer the group leader to the specified server.
   * @param ratisElectionTransferRequest  (optional)
   * @param additionalHeaders additionalHeaders for this call
   * @return HandleResponse
   * @throws ApiException if fails to make API call
   */
  public HandleResponse transferRatisLeader(RatisElectionTransferRequest ratisElectionTransferRequest, Map<String, String> additionalHeaders) throws ApiException {
    Object localVarPostBody = ratisElectionTransferRequest;
    
    // create path and map variables
    String localVarPath = "/api/v1/ratis/election/transfer";

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
