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
import { type DataTableColumns, NButton, type PaginationProps } from 'naive-ui'

defineOptions({ name: 'TenantTable' })

const router = useRouter()

defineProps({
  data: {
    type: Array as PropType<Tenant[]>,
    default: () => []
  },
  pagination: {
    type: Object as PropType<PaginationProps>,
    default: () => ({})
  }
})

const toTenantDetail = (tenantRow: Tenant) => {
  const { tenant, totalApplicationNum, totalShuffleSize } = tenantRow
  router.push({
    name: 'tenantDetail',
    query: { tenant, totalApplicationNum, totalShuffleSize }
  })
}

const columns: DataTableColumns<Tenant> = [
  {
    title: 'Tenant',
    key: 'tenant'
  },
  {
    title: 'TotalShuffleSize',
    key: 'totalShuffleSize'
  },
  {
    title: 'TotalAppNums',
    key: 'totalApplicationNum'
  },
  {
    title: 'More',
    key: 'more',
    render: (row) => {
      return h(
        NButton,
        {
          text: true,
          type: 'primary',
          size: 'small',
          onClick: () => {
            toTenantDetail(row)
          }
        },
        { default: () => 'More' }
      )
    }
  }
]
</script>

<template>
  <n-data-table :columns="columns" :data="data" remote :pagination="pagination" />
</template>
