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
import { getApplicationOverview, getApplicationList } from '@/api'
import { useHasLoading } from '@varlet/axle/use'
import { ApplicationOverviewService } from '@/views/overview/components'
import { ApplicationListService } from '@/views/application/components'
import type { Application } from '@/api/models/application/types'
import type { Page } from '@/api/types'

defineOptions({
  name: 'ApplicationView'
})

const pageInfos = reactive<Page>({
  pageNum: 1,
  pageSize: 10
})

const { use: useApplicationList, load: loadApplicationList } = getApplicationList()

const { data: appOverview, loading: isAppOverviewLoading } = getApplicationOverview().use()
const { data: appResponse, loading: isAppListLoading } = useApplicationList({ params: pageInfos })

const loading = useHasLoading(isAppOverviewLoading, isAppListLoading)

const applicationList = computed<Application[]>(() => appResponse.value?.applicationInfos || [])
const applicationTotalCount = computed<number>(() => appResponse.value?.totalCount || 0)
</script>

<template>
  <n-spin :show="loading">
    <n-flex :style="{ gap: '24px' }" vertical>
      <ApplicationOverviewService :data="appOverview" />
      <ApplicationListService
        :data="applicationList"
        :count="applicationTotalCount"
        :page="pageInfos"
        @do-search="(search) => loadApplicationList(search)"
      />
    </n-flex>
  </n-spin>
</template>
