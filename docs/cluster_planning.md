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

# Cluster Planning

## Node Spec

Empirical size configs for Celeborn nodes
The principle is to try to avoid any hardware(CPU, Memory, Disk Bandwidth/IOPS, Network
Bandwidth/PPS)becoming the bottleneck.
The goal is to let all the hardware usage be close to each other, for example let the disk
IOPS/Bandwidth usage and network usage stay roughly the same so that data will be perfectly
pipelined and no back-pressure will be triggered.

The goal is hard to reach, and perhaps has a relationship with workload characteristics, and also
Celeborn’s configs can have some impact. In our former experience, vCores: memory(GB):  Bandwidth(
Gbps): Disk IO (KIOps) is better to be 2: 5: 2: 1.
We didn’t thoroughly conduct experiments on various configs(it’s hard to do so), so it’s merely a
reference.

## Worker Scale

You need to estimate your cluster's max concurrent shuffle size(ES), and get the total usable disk
space of a node(NS). The worker count can be `(ES * 2 / NS)`.
