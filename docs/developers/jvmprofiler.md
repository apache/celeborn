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

# JVM Profiler
Since version 0.5.0, Celeborn supports JVM sampling profiler to capture CPU and memory profiles. This article provides a detailed guide of Celeborn `Worker`'s code profiling.

## Worker Code Profiling
The JVM profiler enables code profiling of workers based on the [async profiler](https://github.com/async-profiler/async-profiler/blob/v2.10/README.md), a low overhead sampling profiler.
This allows a `Worker` instance to capture CPU and memory profiles for `Worker` which is later analyzed for performance issues. 
The profiler captures [Java Flight Recorder (jfr)](https://access.redhat.com/documentation/es-es/red_hat_build_of_openjdk/17/html/using_jdk_flight_recorder_with_red_hat_build_of_openjdk/openjdk-flight-recorded-overview) files for each worker that can be read by tools like Java Mission Control and Intellij etc.
The profiler writes the jfr files to the `Worker`'s working directory in the `Worker`'s local file system and the files can grow to be large,
so it is advisable that the `Worker` machines have adequate storage.

Code profiling is currently only supported for

*   Linux (x64)
*   Linux (arm64)
*   Linux (musl, x64)
*   MacOS

To get maximum profiling information set the following jvm options for the `Worker` :

```
-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+PreserveFramePointer
```

For more information on async_profiler see the [Async Profiler Manual](https://krzysztofslusarski.github.io/2022/12/12/async-manual.html).

To enable code profiling, enable the code profiling in the configuration.

```properties
celeborn.worker.jvmProfiler.enabled true
```

For more configuration of code profiling refer to `celeborn.worker.jvmProfiler.*`.

### Profiling Configuration Example
```properties
celeborn.worker.jvmProfiler.enabled true
celeborn.worker.jvmProfiler.options event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s
```
