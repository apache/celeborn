/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { PaginationProps } from 'naive-ui'
import type { PaginationType } from '@/api/types'

export interface UsePaginationOptions {
  currentPage?: number
  pageSize?: number
  onLoadData?: Function
}

export function usePagination(options: UsePaginationOptions) {
  const {
    currentPage: defaultCurrentPage = 1,
    pageSize: defaultPageSize = 30,
    onLoadData
  } = options

  const pageSizes = [30, 50, 100]
  const currentPage = ref(defaultCurrentPage)
  const pageSize = ref(defaultPageSize)
  const itemCount = ref(0)
  const searchParams = ref({})

  function mergeParams(formData?: Record<string, string>) {
    return {
      ...formData,
      pageNum: currentPage.value,
      pageSize: pageSize.value
    }
  }

  function onUpdatePage(value: number) {
    currentPage.value = value
    toLoadData()
  }

  function onUpdatePageSize(value: number) {
    currentPage.value = 1
    pageSize.value = value
    toLoadData()
  }

  function resetSearch(formData: Record<string, string>) {
    currentPage.value = defaultCurrentPage
    pageSize.value = defaultPageSize
    itemCount.value = 0

    searchParams.value = formData
    toLoadData()
  }

  function doSearch(formData: Record<string, string>) {
    currentPage.value = 1
    searchParams.value = formData
    toLoadData()
  }

  async function toLoadData() {
    const params = mergeParams(searchParams.value)
    const data = await onLoadData!({ params })
    itemCount.value = data.totalCount
  }

  onMounted(() => {
    toLoadData()
  })

  return {
    currentPage,
    pageSize,
    itemCount,
    doSearch,
    resetSearch,
    pagination: computed<PaginationProps>(() => {
      return {
        pageSizes,
        showSizePicker: true,
        page: currentPage.value,
        pageSize: pageSize.value,
        itemCount: itemCount.value,
        onChange: onUpdatePage,
        onUpdatePageSize
      }
    })
  }
}
