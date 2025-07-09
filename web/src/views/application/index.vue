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
import { getApplicationList, getApplicationOverview } from '@/api'
import type { Application } from '@/api/models/application/types'
import { usePagination } from '@/composables'
import { ApplicationTableService, ApplicationFormService } from '@/views/application/components'
import { ApplicationOverviewService } from '@/views/overview/components'
import { useHasLoading } from '@varlet/axle/use'

defineOptions({
  name: 'ApplicationView'
})

const {
  data: appResponse,
  loading: isAppListLoading,
  getData: loadApplicationList
} = getApplicationList().use({ immediate: false })
const { data: appOverview, loading: isAppOverviewLoading } = getApplicationOverview().use()

const loading = useHasLoading(isAppOverviewLoading, isAppListLoading)

const { pagination, resetSearch, doSearch } = usePagination({
  onLoadData: loadApplicationList
})

const applicationList = computed<Application[]>(() => appResponse.value?.applicationInfos || [])
</script>

<template>
  <n-spin :show="loading">
    <n-flex :style="{ gap: '24px' }" vertical>
      <ApplicationOverviewService :data="appOverview" />
      <ApplicationFormService @reset="resetSearch" @search="doSearch" />
      <ApplicationTableService :data="applicationList" :pagination="pagination" />
    </n-flex>
  </n-spin>
</template>
