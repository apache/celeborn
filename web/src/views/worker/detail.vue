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

<script lang="ts" setup>
import { getApplicationList, getWorkerDetail, type ApplicationTab } from '@/api'
import { usePagination } from '@/composables'
import { useHasLoading } from '@varlet/axle/use'
import { WorkerDetailContentService, WorkerDetailOverviewService } from './components'

defineOptions({
  name: 'WorkerDetail'
})

const route = useRoute()
const worker = route.query.worker as string

const { data, loading: isDetailLoading } = getWorkerDetail().use({
  params: { worker }
})

const {
  data: appResponse,
  loading: isAppListLoading,
  getData: loadApplicationList
} = getApplicationList().use({ immediate: false })

const { pagination } = usePagination({
  onLoadData: loadApplicationList,
  params: {
    worker
  }
})

const application = computed<ApplicationTab>(() => {
  return {
    data: appResponse.value?.applicationInfos ?? [],
    pagination: pagination.value
  }
})

const loading = useHasLoading(isDetailLoading, isAppListLoading)
</script>
<template>
  <n-spin :show="loading">
    <n-flex :style="{ gap: '24px' }" vertical>
      <WorkerDetailOverviewService :data="data" />
      <WorkerDetailContentService :data="data" :application="application" />
    </n-flex>
  </n-spin>
</template>
