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
import type { ApplicationTab, WorkerDetail } from '@/api'
import { ApplicationTableService } from '@/views/application/components'
import type { PropType } from 'vue'
import { WorkerDetailMemoryService, WorkerDetailStorageService } from '.'

defineProps({
  data: {
    type: Object as PropType<WorkerDetail>,
    default: () => ({})
  },
  application: {
    type: Object as PropType<ApplicationTab>,
    default: () => {}
  }
})
</script>
<template>
  <n-card title="" style="margin-bottom: 16px">
    <n-tabs type="line" animated>
      <n-tab-pane name="Storages" tab="Storages">
        <WorkerDetailStorageService :data="data.diskInfos" />
      </n-tab-pane>
      <n-tab-pane name="Memory" tab="Memory">
        <WorkerDetailMemoryService :data="data.memoryInfo" />
      </n-tab-pane>
      <n-tab-pane name="Application" tab="Application">
        <ApplicationTableService :="application" />
      </n-tab-pane>
      <n-tab-pane name="Configuration" tab="Configuration">
        <ConfigurationService :dynamic="data.dynamicConfigs" :static="data.staticConfigs" />
      </n-tab-pane>
      <n-tab-pane name="FlameGraph" tab="FlameGraph"> {{ data.flameGraph }} </n-tab-pane>
      <n-tab-pane name="ThreadDump" tab="ThreadDump"> {{ data.threadDump }} </n-tab-pane>
      <n-tab-pane name="Metrics" tab="Metrics"> {{ data.metrics }} </n-tab-pane>
      <n-tab-pane name="Logs" tab="Logs"> {{ data.logs }} </n-tab-pane>
      <n-tab-pane name="LogList" tab="LogList">
        <LogFilesService :data="data.logFiles" />
      </n-tab-pane>
    </n-tabs>
  </n-card>
</template>
