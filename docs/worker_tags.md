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

# Worker Tags

Worker tags in Celeborn allow users to assign specific tags (labels) to workers 
in the cluster. Tags can help Celeborn grouping together workers with similar 
characteristics, allowing applications with different priorities/users to use 
distinct groups of workers and create isolated sub-clusters.

Use Cases –
  - Workers tagged with different configurations – "hdd-14t", "ssd-245g", "high-nw"
  - Workers tagged with "tier1" to indicate high-performance workers.
  - Workers tagged with different tenant names for isolation.
  - Workers tagged with versions "v0.6.0" can help in controlled rolling upgrades.

## FAQ

#### - What happens if no worker matches the specified tagsExpr?
If no worker matches the specified tags expression, no workers will be selected
for the shuffle. Depending on application configurations it can fall back to 
Spark Shuffle.

#### - Can a worker have multiple tags?
Yes, a worker can have multiple tags.

#### - Are there restrictions on the tag naming format?
Tags should be alphanumeric and can include dashes or underscores. Avoid special
characters to ensure compatibility.

#### - Can tags be updated for a running worker?
Yes, tags can be dynamically updated for a running worker via dynamic config service.