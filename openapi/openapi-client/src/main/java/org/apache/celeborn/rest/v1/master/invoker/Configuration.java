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


package org.apache.celeborn.rest.v1.master.invoker;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class Configuration {
  public static final String VERSION = "0.7.0-SNAPSHOT";

  private static final AtomicReference<ApiClient> defaultApiClient = new AtomicReference<>();
  private static volatile Supplier<ApiClient> apiClientFactory = ApiClient::new;

  /**
   * Get the default API client, which would be used when creating API instances without providing an API client.
   *
   * @return Default API client
   */
  public static ApiClient getDefaultApiClient() {
    ApiClient client = defaultApiClient.get();
    if (client == null) {
      client = defaultApiClient.updateAndGet(val -> {
        if (val != null) { // changed by another thread
          return val;
        }
        return apiClientFactory.get();
      });
    }
    return client;
  }

  /**
   * Set the default API client, which would be used when creating API instances without providing an API client.
   *
   * @param apiClient API client
   */
  public static void setDefaultApiClient(ApiClient apiClient) {
    defaultApiClient.set(apiClient);
  }

  /**
   * set the callback used to create new ApiClient objects
   */
  public static void setApiClientFactory(Supplier<ApiClient> factory) {
    apiClientFactory = Objects.requireNonNull(factory);
  }

  private Configuration() {
  }
}