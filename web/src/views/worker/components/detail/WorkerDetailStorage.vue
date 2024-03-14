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
import type { WorkerDiskInfo } from '@/api'
import type { DataTableColumns } from 'naive-ui'
import type { PropType } from 'vue'

defineProps({
  data: {
    type: Array as PropType<WorkerDiskInfo[]>,
    default: () => []
  }
})

const columns: DataTableColumns<WorkerDiskInfo> = [
  {
    title: 'DiskInfo',
    key: 'index',
    render: (_, index) => {
      return `DiskInfo${index + 1}`
    }
  },
  {
    title: 'MountPoint',
    key: 'mountPoint'
  },
  {
    title: 'TotalSpace',
    key: 'totalSpace'
  },
  {
    title: 'UsedSpace',
    key: 'usedSpace'
  },
  {
    title: 'UsedPercent',
    key: 'usedPercent'
  },
  {
    title: 'DiskType',
    key: 'diskType'
  }
]
</script>
<template>
  <n-data-table scroll-x="800" :columns="columns" :data="data" :pagination="false" />
</template>
