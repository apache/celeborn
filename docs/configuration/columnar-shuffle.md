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

<!--begin-include-->
| Key | Default | Description | Since |
| --- | ------- | ----------- | ----- |
| celeborn.columnar.offHeap.enabled | false | Whether to use off heap columnar vector. | 0.2.0 | 
| celeborn.columnar.shuffle.batch.size | 10000 | Vector batch size for columnar shuffle. | 0.2.0 | 
| celeborn.columnar.shuffle.compression.codec | none | Compression codec used for columnar-based shuffle data. Available options: none. | 0.2.0 | 
| celeborn.columnar.shuffle.enabled | false | Whether to enable columnar-based shuffle. | 0.2.0 | 
| rss.columnar.shuffle.maxDictFactor | 0.3 |  | 0.2.0 | 
<!--end-include-->
