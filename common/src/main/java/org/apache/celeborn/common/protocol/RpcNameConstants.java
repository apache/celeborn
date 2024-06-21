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

public class RpcNameConstants {
  // For Master
  public static String MASTER_SYS = "Master";
  public static String MASTER_INTERNAL_SYS = "MasterInternal";

  // Master Endpoint Name
  public static String MASTER_EP = "MasterEndpoint";
  public static String MASTER_INTERNAL_EP = "MasterInternalEndpoint";

  // For Worker
  public static String WORKER_SYS = "Worker";
  public static String WORKER_INTERNAL_SYS = "WorkerInternal";
  // Worker Endpoint Name
  public static String WORKER_EP = "WorkerEndpoint";
  public static String WORKER_INTERNAL_EP = "WorkerInternalEndpoint";

  // For Web
  public static String WEB_SYS = "Web";

  // Web Endpoint Name
  public static String WEB_EP = "WebEndpoint";

  // For LifecycleManager
  public static String LIFECYCLE_MANAGER_SYS = "LifecycleManager";
  public static String LIFECYCLE_MANAGER_MASTER_SYS = "LifecycleManagerMasterSys";
  public static String LIFECYCLE_MANAGER_WORKER_SYS = "LifecycleManagerWorkerSys";
  public static String LIFECYCLE_MANAGER_EP = "LifecycleManagerEndpoint";

  // For Shuffle Client
  public static String SHUFFLE_CLIENT_SYS = "ShuffleClient";
}
