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

package org.apache.celeborn.rest.v1.worker.invoker;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Collections;
import java.util.Map;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public abstract class BaseApi {

  protected ApiClient apiClient;

  public BaseApi() {
    this(Configuration.getDefaultApiClient());
  }

  public BaseApi(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  public ApiClient getApiClient() {
    return apiClient;
  }

  public void setApiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @throws ApiException if fails to make API call.
   */
  public void invokeAPI(String url, String method) throws ApiException {
    invokeAPI(url, method, null, null, Collections.emptyMap());
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param additionalHeaders Additional headers for the request.
   * @throws ApiException if fails to make API call.
   */
  public void invokeAPI(String url, String method, Map<String, String> additionalHeaders) throws ApiException {
    invokeAPI(url, method, null, null, additionalHeaders);
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param request The request object.
   * @throws ApiException if fails to make API call.
   */
  public void invokeAPI(String url, String method, Object request) throws ApiException {
    invokeAPI(url, method, request, null, Collections.emptyMap());
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param request The request object.
   * @param additionalHeaders Additional headers for the request.
   * @throws ApiException if fails to make API call.
   */
  public void invokeAPI(String url, String method, Object request, Map<String, String> additionalHeaders) throws ApiException {
    invokeAPI(url, method, request, null, additionalHeaders);
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param returnType The return type.
   * @return The API response in the specified type.
   * @throws ApiException if fails to make API call.
   */
  public <T> T invokeAPI(String url, String method, TypeReference<T> returnType) throws ApiException {
    return invokeAPI(url, method, null, returnType, Collections.emptyMap());
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param request The request object.
   * @param returnType The return type.
   * @return The API response in the specified type.
   * @throws ApiException if fails to make API call.
   */
  public <T> T invokeAPI(String url, String method, Object request, TypeReference<T> returnType) throws ApiException {
    return invokeAPI(url, method, request, returnType, Collections.emptyMap());
  }

  /**
   * Directly invoke the API for the given URL. Useful if the API returns direct links/URLs for subsequent requests.
   * @param url The URL for the request, either full URL or only the path.
   * @param method The HTTP method for the request.
   * @param request The request object.
   * @param returnType The return type.
   * @param additionalHeaders Additional headers for the request.
   * @return The API response in the specified type.
   * @throws ApiException if fails to make API call.
   */
  public abstract <T> T invokeAPI(String url, String method, Object request, TypeReference<T> returnType, Map<String, String> additionalHeaders) throws ApiException;
}
