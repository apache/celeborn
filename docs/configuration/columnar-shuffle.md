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
| Key | Default | Description | Since | Deprecated |
| --- | ------- | ----------- | ----- | ---------- |
| celeborn.columnarShuffle.batch.size | 10000 | Vector batch size for columnar shuffle. | 0.3.0 | celeborn.columnar.shuffle.batch.size | 
| celeborn.columnarShuffle.codegen.enabled | false | Whether to use codegen for columnar-based shuffle. | 0.3.0 | celeborn.columnar.shuffle.codegen.enabled | 
| celeborn.columnarShuffle.enabled | false | Whether to enable columnar-based shuffle. | 0.2.0 | celeborn.columnar.shuffle.enabled | 
| celeborn.columnarShuffle.encoding.dictionary.enabled | false | Whether to use dictionary encoding for columnar-based shuffle data. | 0.3.0 | celeborn.columnar.shuffle.encoding.dictionary.enabled | 
| celeborn.columnarShuffle.encoding.dictionary.maxFactor | 0.3 | Max factor for dictionary size. The max dictionary size is `min(32.0 KiB, celeborn.columnarShuffle.batch.size * celeborn.columnar.shuffle.encoding.dictionary.maxFactor)`. | 0.3.0 | celeborn.columnar.shuffle.encoding.dictionary.maxFactor | 
| celeborn.columnarShuffle.offHeap.enabled | false | Whether to use off heap columnar vector. | 0.3.0 | celeborn.columnar.offHeap.enabled | 
<!--end-include-->
