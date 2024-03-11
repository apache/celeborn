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
import type { Worker } from '@/api'
import type { DataTableColumns, PaginationProps } from 'naive-ui'
import type { PropType } from 'vue'

defineOptions({
  name: 'WorkerTable'
})

defineProps({
  data: {
    type: Array as PropType<Array<Worker>>,
    default: () => []
  },
  pagination: {
    type: Object as PropType<PaginationProps>,
    default: () => ({})
  }
})

const columns: DataTableColumns<Worker> = [
  {
    title: 'HostName',
    key: 'hostname'
  },
  {
    title: 'IP',
    key: 'ip'
  },
  {
    title: 'State',
    key: 'state'
  },
  {
    title: 'StartTime',
    key: 'startTime'
  },
  {
    title: 'HeartbeatTime',
    key: 'heartbeatTime'
  },
  {
    title: 'RPCPort',
    key: 'rpcPort'
  },
  {
    title: 'RestPort',
    key: 'restPort'
  },
  {
    title: 'FetchPort',
    key: 'fetchPort'
  },
  {
    title: 'PushPort',
    key: 'pushPort'
  },
  {
    title: 'ReplicatePort',
    key: 'replicatePort'
  },
  {
    title: 'Version',
    key: 'version'
  },
  {
    title: 'More',
    key: 'more'
  }
]
</script>

<template>
  <n-data-table :columns="columns" :data="data" remote :pagination="pagination" />
</template>
