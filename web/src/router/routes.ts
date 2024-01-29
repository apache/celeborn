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

import overview_routes from './modules/overview'
import master_routes from './modules/master'
import worker_routes from './modules/worker'
import application_routes from './modules/application'
import tenant_routes from './modules/tenant'

export const children_routes = [
  overview_routes,
  master_routes,
  worker_routes,
  application_routes,
  tenant_routes
]

export const basePage = [
  {
    path: '/',
    redirect: { name: 'overview' },
    sub: true,
    component: () => import('@/layouts/layoutPage.vue'),
    children: children_routes
  }
]

const routes = [...basePage]

export default routes
