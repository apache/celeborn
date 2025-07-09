---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Overview
Celeborn Client is separated into [two roles](../../developers/overview#components):

- `LifecycleManager` for control plane, responsible for managing all shuffle metadata for the application, resides
  in driver for Apache Spark and JobMaster for Apache Flink. See [LifecycleManager](../../developers/lifecyclemanager)
- `ShuffleClient` for data plane, responsible for write/read data to/from Workers, resides in executors for Apache
  Spark and TaskManager for Apache Flink. See [ShuffleClient](../../developers/shuffleclient)