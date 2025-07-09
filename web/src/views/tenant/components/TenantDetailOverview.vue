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
import type { PropType } from 'vue'
import type { Tenant } from '@/api'

defineOptions({ name: 'TenantDetailOverview' })

defineProps({
  data: {
    type: Object as PropType<Tenant>,
    default: () => ({})
  }
})
</script>

<template>
  <n-card hoverable>
    <n-descriptions label-placement="left" title="Tenant Detail" :column="4">
      <n-descriptions-item label="Tenant">
        {{ data.tenant }}
      </n-descriptions-item>
      <n-descriptions-item label="TotalShuffleSize">
        {{ data.totalShuffleSize }}
      </n-descriptions-item>
      <n-descriptions-item label="TotalAppNums">
        {{ data.totalApplicationNum }}
      </n-descriptions-item>
    </n-descriptions>
  </n-card>
</template>
