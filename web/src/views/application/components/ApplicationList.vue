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
import type { DataTableColumns } from 'naive-ui'
import type { Application, ApplicationSearchModel } from '@/api/models/application/types'
import type { Page } from '@/api/types'

defineOptions({
  name: 'ApplicationDataTable'
})

const $props = defineProps({
  data: {
    type: Array as PropType<Application[]>,
    default: () => []
  },
  count: {
    type: Number as PropType<number>,
    default: 0
  },
  page: {
    type: Object as PropType<Page>,
    default: () => {
      return { pageSize: 10, pageNum: 1 }
    }
  }
})

const $emits = defineEmits(['do-search'])

const searchFormModel = ref<ApplicationSearchModel>({
  pageNum: $props.page.pageNum,
  pageSize: $props.page.pageSize
})

const doSearch = () => {
  $emits('do-search', searchFormModel.value)
}

const resetSearch = () => {
  searchFormModel.value.pageNum = 1
  searchFormModel.value.applicationId = ''
  $emits('do-search', searchFormModel.value)
}

const columns: DataTableColumns<Application> = [
  {
    title: 'ApplicationId',
    key: 'appId',
    sorter: true
  },
  {
    title: 'SubUser',
    key: 'subUser',
    sorter: true
  },
  {
    title: 'Tenant',
    key: 'tenant'
  },
  {
    title: 'HeartbeatTime',
    key: 'heartbeatTime'
  },
  {
    title: 'ShuffleSize',
    key: 'shuffleSize'
  },
  {
    title: 'ActiveShuffle',
    key: 'shuffleFileCount',
    sorter: true
  }
]

const pagination = reactive({
  page: searchFormModel.value.pageNum,
  pageSize: searchFormModel.value.pageSize,
  itemCount: $props.count,
  showSizePicker: true,
  pageSizes: [10, 20, 40],
  onChange: (page: number) => {
    searchFormModel.value.pageNum = pagination.page = page
    doSearch()
  },
  onUpdatePageSize: (pageSize: number) => {
    searchFormModel.value.pageSize = pagination.pageSize = pageSize
    searchFormModel.value.pageNum = pagination.page = 1
    doSearch()
  }
})
</script>

<template>
  <n-form
    :model="searchFormModel"
    :show-feedback="false"
    label-placement="left"
    @keydown.enter="doSearch"
  >
    <n-grid :x-gap="24" :y-gap="24" :cols="4">
      <n-form-item-gi label="ApplicationId" path="applicationId">
        <n-input
          v-model:value="searchFormModel.applicationId"
          placeholder="Please input the applicationId"
          clearable
        />
      </n-form-item-gi>
      <n-grid-item>
        <n-flex>
          <n-button type="primary" ghost @click="doSearch">Search</n-button>
          <n-button ghost @click="resetSearch">Reset</n-button>
        </n-flex>
      </n-grid-item>
    </n-grid>
  </n-form>
  <n-data-table :columns="columns" :data="data" :pagination="pagination" />
</template>
