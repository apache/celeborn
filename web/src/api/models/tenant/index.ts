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

import { api } from '@/api/request'
import type { PaginationType } from '@/api/types'
import type { TenantOverview, Tenant, TenantDetail } from './types'

export * from './types'

export const getTenantOverview = () => {
  return api<TenantOverview>('/tenant/overview', 'get')
}

export const getTenantList = () => {
  return api<PaginationType<{ tenantInfos: Tenant[] }>>('/tenant/list', 'get')
}

export const getTenantSubUserList = () => {
  return api<PaginationType<{ subUserInfos: TenantDetail[] }>>('/tenant/subuser/list', 'get')
}
