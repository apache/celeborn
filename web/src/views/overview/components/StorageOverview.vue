<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

<template>
  <n-card hoverable>
    <n-descriptions label-placement="left" title="Storage Usage Infos">
      <n-descriptions-item label="Disk"> {{ data.diskUsage }} </n-descriptions-item>
      <n-descriptions-item label="Memory">
        {{ data.memoryUsage }}
      </n-descriptions-item>
      <n-descriptions-item label="HDFS">
        {{ data.hdfsUsage }}
      </n-descriptions-item>
    </n-descriptions>
  </n-card>
</template>
<script lang="ts" setup>
import type { StorageOverview } from '@/api'
import type { PropType } from 'vue'

defineProps({
  data: {
    type: Object as PropType<StorageOverview>,
    default: () => ({
      diskUsage: '',
      memoryUsage: '',
      hdfsUsage: ''
    })
  }
})
</script>
