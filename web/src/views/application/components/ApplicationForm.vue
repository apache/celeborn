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
  name: 'ApplicationForm',
  setup(_, ctx) {
    const searchForm = reactive({
      appId: '',
      subUser: '',
      tenant: ''
    })

    const { doSearch, resetSearch } = useFormEvent(searchForm, ctx)

    return {
      searchForm,
      doSearch,
      resetSearch
    }
  }
})
</script>

<template>
  <n-form
    :model="searchForm"
    :show-feedback="false"
    label-placement="left"
    @keydown.enter="doSearch"
  >
    <n-grid :x-gap="24" :y-gap="24" cols="1 400:2 800:4 1200:6">
      <n-form-item-gi label="AppId" path="appId">
        <n-input v-model:value="searchForm.appId" placeholder="" clearable />
      </n-form-item-gi>
      <n-form-item-gi label="SubUser" path="subUser">
        <n-input v-model:value="searchForm.subUser" placeholder="" clearable />
      </n-form-item-gi>
      <n-form-item-gi label="Tenant" path="tenant">
        <n-input v-model:value="searchForm.tenant" placeholder="" clearable />
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
