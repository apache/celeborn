---
hide:
  - navigation

license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Migration Guide

## Upgrading from 0.2.1 to 0.3.0

 - From 0.3.0 on the default value for `celeborn.client.push.replicate.enabled` is changed from `true` to `false`, users
   who want replication on should explicitly enable replication. For example, to enable replication for Spark
   users should add the spark config when submitting job: `spark.celeborn.client.push.replicate.enabled=true`

 - From 0.3.0 on the default value for `celeborn.worker.storage.workingDir` is changed from `hadoop/rss-worker/shuffle_data` to `rss-worker/shuffle_data`,
   users who want to use origin working dir path should set this configuration.

 - Since 0.3.0, configuration namespace `celeborn.ha.master` is deprecated, and will be removed in the future versions.
   All configurations `celeborn.ha.master.*` should migrate to `celeborn.master.ha.*`.

 - Since 0.3.0, environment variables `CELEBORN_MASTER_HOST` and `CELEBORN_MASTER_PORT` are removed.
   Instead `CELEBORN_LOCAL_HOSTNAME` works on both master and worker, which takes high priority than configurations defined in properties file.

 - Since 0.3.0, the Celeborn Master URL schema is changed from `rss://` to `celeborn://`, for users who start Worker by
   `sbin/start-worker.sh rss://<master-host>:<master-port>`, should migrate to `sbin/start-worker.sh celeborn://<master-host>:<master-port>`.

 - When using 0.2.1 as client side and 0.3.0 as server side, you may see the following Exception in LifecycleManger's
   log. You can safely ignore the log, it's caused by the behavior change when Master receives heartbeat from Application.

    ??? warning "logs"
        ```
        23/06/20 18:12:30 WARN TransportChannelHandler: Exception in connection from /192.168.1.16:9097
        java.io.InvalidObjectException: enum constant HEARTBEAT_FROM_APPLICATION_RESPONSE does not exist in class org.apache.celeborn.common.protocol.MessageType
            at java.io.ObjectInputStream.readEnum(ObjectInputStream.java:2157)
            at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1662)
            at java.io.ObjectInputStream.defaultReadFields(ObjectInputStream.java:2430)
            at java.io.ObjectInputStream.readSerialData(ObjectInputStream.java:2354)
            at java.io.ObjectInputStream.readOrdinaryObject(ObjectInputStream.java:2212)
            at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1668)
            at java.io.ObjectInputStream.readObject(ObjectInputStream.java:502)
            at java.io.ObjectInputStream.readObject(ObjectInputStream.java:460)
            at org.apache.celeborn.common.serializer.JavaDeserializationStream.readObject(JavaSerializer.scala:76)
            at org.apache.celeborn.common.serializer.JavaSerializerInstance.deserialize(JavaSerializer.scala:110)
        ```
