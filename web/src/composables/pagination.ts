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

export interface UsePaginationOptions {
  search?: string
  current_page?: number
  page_size?: number
  page_count?: number
  item_count?: number
  onLoadData?: Function
}

export function usePagination(options: UsePaginationOptions = {}) {
  const search = ref(options.search ?? '')
  const current_page = ref(options.current_page ?? 1)
  const page_size = ref(options.page_size ?? 20)
  const page_count = ref(options.page_count ?? 0)
  const item_count = ref(options.item_count ?? 0)

  function onUpdatePageChange(value: number) {
    current_page.value = value
    options.onLoadData!()
  }

  function onUpdateSizeChange(value: number) {
    page_size.value = value
    options.onLoadData!()
  }

  function reset() {
    current_page.value = options.current_page ?? 1
    page_size.value = options.page_size ?? 20
    page_count.value = options.page_count ?? 0
    item_count.value = options.item_count ?? 0
  }

  return {
    search,
    current_page,
    page_size,
    page_count,
    item_count,
    reset,
    props: computed(() => {
      return {
        current_page: current_page.value,
        page_size: page_size.value,
        page_count: page_count.value,
        item_count: item_count.value
      }
    }),
    emits: {
      'update:page': onUpdatePageChange,
      'update:pageSize': onUpdateSizeChange
    }
  }
}
