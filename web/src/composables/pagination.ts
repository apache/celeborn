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

export interface UsePaginationOptions<T> {
  pageNum?: number
  pageSize?: number
  pageSizes?: number[]
  pageCount?: number
  totalCount?: number
  onLoadData?(params?: Record<string, any>): Promise<PaginationType<T>>
}

export type SearchForm = Record<string, string>

export function usePagination<T>(options: UsePaginationOptions<T> = {}) {
  const {
    pageNum: defaultPageNum = 1,
    pageSize: defaultPageSize = 30,
    pageSizes: defaultPageSizes = [30, 50, 100],
    pageCount: defaultPageCount = 0,
    totalCount: defaultTotalCount = 0,
    onLoadData
  } = options

  const searchParams = ref<SearchForm | undefined>({})
  const pageNum = ref(defaultPageNum)
  const pageSize = ref(defaultPageSize)
  const pageSizes = ref(defaultPageSizes)
  const pageCount = ref(defaultPageCount)
  const totalCount = ref(defaultTotalCount)

  function mergeParams(searchForm?: SearchForm) {
    searchParams.value = searchForm
    return { params: { pageNum: pageNum.value, pageSize: pageSize.value, ...(searchForm || {}) } }
  }

  function onUpdatePageChange(value: number) {
    pageNum.value = value
    onLoadData!(mergeParams(searchParams.value))
  }

  function onUpdateSizeChange(value: number) {
    pageSize.value = value
    onLoadData!(mergeParams(searchParams.value))
  }

  async function reset(newSearch?: SearchForm) {
    pageNum.value = defaultPageNum
    pageSize.value = defaultPageSize
    pageCount.value = defaultPageCount
    const { totalCount: total } = await onLoadData!(mergeParams(newSearch))
    totalCount.value = total || 0
    pageCount.value = Math.ceil(totalCount.value / pageSize.value)
  }

  async function search(newSearch?: SearchForm) {
    pageNum.value = 1
    const { totalCount: total } = await onLoadData!(mergeParams(newSearch))
    totalCount.value = total || 0
    pageCount.value = Math.ceil(totalCount.value / pageSize.value)
  }

  onMounted(() => {
    search()
  })

  return {
    searchParams,
    pageNum,
    pageSize,
    pageSizes,
    pageCount,
    totalCount,
    search,
    reset,
    pagination: computed<PaginationProps>(() => {
      return {
        page: pageNum.value,
        pageSize: pageSize.value,
        pageSizes: pageSizes.value,
        pageCount: pageCount.value,
        showSizePicker: true,
        onChange: onUpdatePageChange,
        onPageSizeChange: onUpdateSizeChange
      }
    })
  }
}
