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

package org.apache.celeborn.common.identity

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class DefaultIdentityProviderSuite extends CelebornFunSuite {
  test("provide() should use provided CelebornConf") {
    val conf = new CelebornConf()

    val TEST_TENANT_ID = "test-id"
    val TEST_TENANT_NAME = "test-user"

    conf.set(CelebornConf.USER_SPECIFIC_TENANT, TEST_TENANT_ID)
    conf.set(CelebornConf.USER_SPECIFIC_USERNAME, TEST_TENANT_NAME)

    val defaultIdentityProvider = new DefaultIdentityProvider(conf)
    val userIdentifier = defaultIdentityProvider.provide()

    assert(userIdentifier.tenantId == TEST_TENANT_ID)
    assert(userIdentifier.name == TEST_TENANT_NAME)
  }

  test("provide() should use default CelebornConf if not provided") {
    val conf = new CelebornConf()
    val defaultIdentityProvider = new DefaultIdentityProvider(conf)

    val DEFAULT_TENANT_ID = "default"
    val DEFAULT_USERNAME = "default"

    val userIdentifier = defaultIdentityProvider.provide()

    assert(userIdentifier.tenantId == DEFAULT_TENANT_ID)
    assert(userIdentifier.name == DEFAULT_USERNAME)
  }
}
