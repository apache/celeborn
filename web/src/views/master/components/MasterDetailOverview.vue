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

<script setup lang="ts">
import type { MasterDetail } from '@/api'
import type { PropType } from 'vue'

defineProps({
  data: {
    type: Object as PropType<MasterDetail>,
    default: () => ({})
  }
})
</script>
<template>
  <n-card hoverable>
    <n-descriptions label-placement="left" title="Master Detail" :column="4">
      <n-descriptions-item label="HostName"> {{ data.hostname }} </n-descriptions-item>
      <n-descriptions-item label="State"> {{ data.state }} </n-descriptions-item>
      <n-descriptions-item label="AvailableSlots"> {{ data.availableSlots }} </n-descriptions-item>
      <n-descriptions-item label="PartitionSize"> {{ data.partitionSize }} </n-descriptions-item>
      <n-descriptions-item label="WorkerNum"> {{ data.workerNum }} </n-descriptions-item>
      <n-descriptions-item label="ApplicationNum"> {{ data.applicationNum }} </n-descriptions-item>
    </n-descriptions>
  </n-card>
</template>
