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

<script lang="ts">
import { useFormEvent } from '@/composables'

export default defineComponent({
  name: 'WorkerForm',
  setup(_, ctx) {
    const formData = reactive({
      ip: '',
      state: ''
    })
    const { doSearch, resetSearch } = useFormEvent(formData, ctx)
    return {
      formData,
      doSearch,
      resetSearch
    }
  }
})
</script>

<template>
  <n-form :model="formData" :show-feedback="false" label-placement="left" @keydown.enter="doSearch">
    <n-grid :x-gap="24" :y-gap="24" cols="1 400:2 800:4 1200:6">
      <n-form-item-gi label="IP" path="ip">
        <n-input v-model:value="formData.ip" placeholder="" clearable />
      </n-form-item-gi>
      <n-form-item-gi label="State" path="state">
        <n-select v-model:value="formData.state" placeholder="" clearable />
      </n-form-item-gi>
      <n-grid-item>
        <n-flex>
          <n-button type="primary" ghost @click="doSearch">Search</n-button>
          <n-button ghost @click="resetSearch">Reset</n-button>
        </n-flex>
      </n-grid-item>
    </n-grid>
  </n-form>
</template>
