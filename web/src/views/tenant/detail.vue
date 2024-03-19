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
import { getTenantDetail } from '@/api'
import { useHasLoading } from '@varlet/axle/use'
import { TenantDetailOverviewService } from './components'

defineOptions({ name: 'TenantDetail' })

const route = useRoute()
const tenant = route.query.tenant as string

const { data, loading: isDetailLoading } = getTenantDetail().use({
  params: { tenant }
})

const loading = useHasLoading(isDetailLoading)
</script>

<template>
  <n-spin :show="loading">
    <n-flex :style="{ gap: '24px' }" vertical>
      <TenantDetailOverviewService :data="data" />
    </n-flex>
  </n-spin>
</template>
