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
import { QuestionCircleOutlined } from '@vicons/antd'
import {
  NButton,
  NIcon,
  NSpace,
  NTooltip,
  type DataTableColumns,
  type PaginationProps
} from 'naive-ui'
import type { PropType } from 'vue'

const router = useRouter()

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

const toWorkerDetail = ({ ip, rpcPort, pushPort, fetchPort, replicatePort }: Worker) => {
  const worker = `${ip}:${rpcPort}:${pushPort}:${fetchPort}:${replicatePort}`
  router.push({
    name: 'workerDetail',
    query: { worker }
  })
}

const renderToolTip = () => {
  return h(NSpace, null, {
    default: () => [
      h('span', null, { default: () => 'Ports' }),
      h(NTooltip, null, {
        default: () => 'RpcPort:PushPort:FetchPort:ReplicatePort',
        trigger: () =>
          h(NIcon, { style: { paddingTop: '4px' } }, { default: () => h(QuestionCircleOutlined) })
      })
    ]
  })
}

const columns: DataTableColumns<Worker> = [
  {
    title: 'Hostname/IP',
    key: 'hostname',
    render({ hostname, ip }) {
      return `${hostname}/${ip}`
    }
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
    key: 'ports',
    title: renderToolTip,
    render({ rpcPort, pushPort, fetchPort, replicatePort }) {
      const ports = `${rpcPort}:${pushPort}:${fetchPort}:${replicatePort}`
      return ports
    }
  },
  {
    title: 'Version',
    key: 'version'
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
            toWorkerDetail(row)
          }
        },
        { default: () => 'More' }
      )
    }
  }
]
</script>

<template>
  <n-data-table :scroll-x="1000" :columns="columns" :data="data" remote :pagination="pagination" />
</template>
