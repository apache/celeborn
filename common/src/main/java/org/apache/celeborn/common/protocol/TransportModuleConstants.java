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

package org.apache.celeborn.common.protocol;

public class TransportModuleConstants {
  public static final String PUSH_MODULE = "push";
  public static final String REPLICATE_MODULE = "replicate";
  public static final String FETCH_MODULE = "fetch";

  // RPC module used by the application components to communicate with each other
  // This is used only at the application side.
  // This is interally further split into RPC_LIFECYCLEMANAGER_MODULE and
  // RPC_APP_CLIENT_MODULE - both of which inherit from RPC_APP_MODULE
  // So for users, there is only RPC_APP_MODULE
  public static final String RPC_APP_MODULE = "rpc_app";
  // RPC module used to communicate with/between server components
  // This is used both at server (master/worker) and application side.
  public static final String RPC_SERVICE_MODULE = "rpc_service";

  // See RPC_APP_MODULE for details - both RPC_LIFECYCLEMANAGER_MODULE and RPC_APP_CLIENT_MODULE
  // are internal modules, and transport configs are not expected for these.
  // For example, auto-ssl requires both RPC_LIFECYCLEMANAGER_MODULE and RPC_APP_CLIENT_MODULE
  // to be in sync, though it is enabled only in RPC_LIFECYCLEMANAGER_MODULE
  public static final String RPC_LIFECYCLEMANAGER_MODULE = "rpc_app_lifecyclemanager";
  public static final String RPC_APP_CLIENT_MODULE = "rpc_app_client";

  // Both RPC_APP and RPC_SERVER fallsback to earlier RPC_MODULE for backward
  // compatibility
  @Deprecated public static final String RPC_MODULE = "rpc";

  public static final String DATA_MODULE = "data";

  public static final String WILDCARD_BIND_ADDRESS = null;
}
