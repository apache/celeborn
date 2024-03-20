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
import { getTenantSubUserList, type Tenant } from '@/api'
import { useHasLoading } from '@varlet/axle/use'
import { TenantDetailTableService, TenantDetailOverviewService } from './components'
import { usePagination } from '@/composables'

defineOptions({ name: 'TenantDetail' })

const route = useRoute()

const tenant = route.query.tenant as string
const totalApplicationNum = Number(route.query.totalApplicationNum).valueOf()
const totalShuffleSize = route.query.totalShuffleSize as string

const tenantData: Tenant = { tenant, totalApplicationNum, totalShuffleSize }
const {
  data: tenantSubUserResponse,
  loading: isDetailLoading,
  getData: loadTenantSubUserList
} = getTenantSubUserList().use({
  params: { tenant: tenantData.tenant },
  immediate: false
})

const { pagination } = usePagination({
  onLoadData: loadTenantSubUserList,
  params: {
    tenant: tenantData.tenant
  }
})

const loading = useHasLoading(isDetailLoading)
</script>

<template>
  <n-spin :show="loading">
    <n-flex :style="{ gap: '24px' }" vertical>
      <TenantDetailOverviewService :data="tenantData" />
      <TenantDetailTableService
        :data="tenantSubUserResponse?.subUserInfos"
        :pagination="pagination"
      />
    </n-flex>
  </n-spin>
</template>
