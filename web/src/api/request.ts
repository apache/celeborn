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

import {
  createAxle,
  requestHeadersInterceptor,
  type RunnerMethod,
  type AxleRequestConfig
} from '@varlet/axle'
import { createUseAxle } from '@varlet/axle/use'
import type { Options } from './types'

export const axle = createAxle({
  baseURL: '/api'
})

axle.useRequestInterceptor(
  requestHeadersInterceptor({
    headers: () => ({
      Authorization: ''
    })
  })
)

axle.useResponseInterceptor({
  onFulfilled(response) {
    const { status, message } = response.data
    if (status !== 200 && message) {
      // do something there
      return Promise.reject(response.data)
    }
    return response.data
  },
  onRejected(error) {
    if (!error.response) return Promise.reject(error)
  }
})

export const useAxle = createUseAxle({
  axle,
  onTransform: (response) => response,
  immediate: true
})

export function api<V, P = Record<string, any>, R = V>(url: string, method: RunnerMethod) {
  function load(params?: P, config?: AxleRequestConfig): Promise<V> {
    return axle[method](url, params, config)
  }

  function use(options: Options<V, R, P> = {}) {
    const [data, getData, extra] = useAxle({ url, method, ...options })
    return {
      data,
      getData,
      ...extra
    }
  }

  return {
    url,
    load,
    use
  }
}
