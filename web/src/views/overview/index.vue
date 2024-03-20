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
import {
  getApplicationOverview,
  getClusterOverview,
  getMasterOverview,
  getStorageOverview,
  getWorkerOverview
} from '@/api'
import { useHasLoading } from '@varlet/axle/use'
import {
  ApplicationOverviewService,
  ClusterOverviewService,
  MasterOverviewService,
  StorageOverviewService,
  WorkerOverviewService
} from './components'

defineOptions({
  name: 'OverView'
})

const { data: cluster, loading: isClusterLoading } = getClusterOverview().use()
const { data: master, loading: isMasterLoading } = getMasterOverview().use()
const { data: application, loading: isApplicationLoading } = getApplicationOverview().use()
const { data: storage, loading: isStorageLoading } = getStorageOverview().use()
const { data: worker, loading: isWorkerLoading } = getWorkerOverview().use()

const loading = useHasLoading(
  isClusterLoading,
  isMasterLoading,
  isApplicationLoading,
  isStorageLoading,
  isWorkerLoading
)
</script>

<template>
  <n-spin :show="loading">
    <n-grid :x-gap="24" :y-gap="24" :cols="2">
      <n-grid-item :span="2">
        <ClusterOverviewService :data="cluster" />
      </n-grid-item>
      <n-grid-item>
        <MasterOverviewService :data="master" />
      </n-grid-item>
      <n-grid-item>
        <ApplicationOverviewService :data="application" />
      </n-grid-item>
      <n-grid-item>
        <WorkerOverviewService :data="worker" />
      </n-grid-item>
      <n-grid-item>
        <StorageOverviewService :data="storage" />
      </n-grid-item>
    </n-grid>
  </n-spin>
</template>
