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
| Key | Default | isDynamic | Description | Since | Deprecated |
| --- | ------- | --------- | ----------- | ----- | ---------- |
| celeborn.columnarShuffle.batch.size | 10000 | false | Vector batch size for columnar shuffle. | 0.3.0 | celeborn.columnar.shuffle.batch.size | 
| celeborn.columnarShuffle.batch.sizeInBytes.enabled | false | false | Whether to also push a columnar batch once its estimated size in bytes reaches `celeborn.columnarShuffle.batch.sizeInBytes.threshold`, in addition to the row limit `celeborn.columnarShuffle.batch.size`. This limits how much row data each partition buffers when rows are wide, e.g. with long strings. Column buffers are still pre-sized for `celeborn.columnarShuffle.batch.size` rows. | 1.0.0 |  | 
| celeborn.columnarShuffle.batch.sizeInBytes.threshold | 64k | false | Estimated size in bytes at which a columnar batch is pushed, when `celeborn.columnarShuffle.batch.sizeInBytes.enabled` is true. The size is estimated from the uncompressed size of the rows added to the batch. The default matches `celeborn.client.push.buffer.max.size`, the per-partition buffer of the row-based hash writer. | 1.0.0 |  | 
| celeborn.columnarShuffle.codegen.enabled | false | false | Whether to use codegen for columnar-based shuffle. | 0.3.0 | celeborn.columnar.shuffle.codegen.enabled | 
| celeborn.columnarShuffle.enabled | false | false | Whether to enable columnar-based shuffle. | 0.2.0 | celeborn.columnar.shuffle.enabled | 
| celeborn.columnarShuffle.encoding.dictionary.enabled | false | false | Whether to use dictionary encoding for columnar-based shuffle data. | 0.3.0 | celeborn.columnar.shuffle.encoding.dictionary.enabled | 
| celeborn.columnarShuffle.encoding.dictionary.maxFactor | 0.3 | false | Max factor for dictionary size. The max dictionary size is `min(32.0 KiB, celeborn.columnarShuffle.batch.size * celeborn.columnar.shuffle.encoding.dictionary.maxFactor)`. | 0.3.0 | celeborn.columnar.shuffle.encoding.dictionary.maxFactor | 
| celeborn.columnarShuffle.offHeap.enabled | false | false | Whether to use off heap columnar vector. | 0.3.0 | celeborn.columnar.offHeap.enabled | 
<!--end-include-->
