package org.apache.celeborn.common.identity

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class DefaultIdentityProviderSuite extends CelebornFunSuite {
  test("provide() without parameters should use default CelebornConf") {
    val defaultIdentityProvider = new DefaultIdentityProvider()
    val DEFAULT_TENANT_ID = "default"
    val DEFAULT_USERNAME = "default"

    val userIdentifier = defaultIdentityProvider.provide()

    assert(userIdentifier.tenantId == DEFAULT_TENANT_ID)
    assert(userIdentifier.name == DEFAULT_USERNAME)
  }

  test("provide(celebornConf) should use provided CelebornConf") {
    val defaultIdentityProvider = new DefaultIdentityProvider()
    val conf = new CelebornConf()

    val TEST_TENANT_ID = "test-id"
    val TEST_TENANT_NAME = "test-user"

    conf.set(CelebornConf.USER_SPECIFIC_TENANT, TEST_TENANT_ID)
    conf.set(CelebornConf.USER_SPECIFIC_USERNAME, TEST_TENANT_NAME)

    val userIdentifier = defaultIdentityProvider.provide(conf)

    assert(userIdentifier.tenantId == TEST_TENANT_ID)
    assert(userIdentifier.name == TEST_TENANT_NAME)
  }
}
