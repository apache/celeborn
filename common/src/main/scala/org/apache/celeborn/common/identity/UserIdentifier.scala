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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging

trait Identifier

case class SystemDefaultIdentifier() extends Identifier {
  override def toString: String = {
    s"SystemDefault"
  }
}

case class TenantIdentifier(tenantId: String) extends Identifier {
  override def toString: String = {
    s"Tenant(`$tenantId`)"
  }
}

case class UserIdentifier(tenantId: String, name: String) extends Identifier {
  assert(
    tenantId != null && tenantId.nonEmpty,
    "UserIdentifier's tenantId should not be null or empty.")
  assert(name != null && name.nonEmpty, "UserIdentifier's name should not be null or empty.")

  def toMap: Map[String, String] = {
    Map("tenantId" -> tenantId, "name" -> name)
  }

  def toJMap: JMap[String, String] = toMap.asJava

  override def toString: String = {
    s"User(`$tenantId`.`$name`)"
  }

  def toTenantIdentifier(): TenantIdentifier = {
    TenantIdentifier(tenantId)
  }
}

object UserIdentifier extends Logging {
  val USER_IDENTIFIER = "^\\`(.+)\\`\\.\\`(.+)\\`$".r

  def apply(userIdentifier: String): UserIdentifier = {
    if (USER_IDENTIFIER.findPrefixOf(userIdentifier).isDefined) {
      val USER_IDENTIFIER(tenantId, name) = userIdentifier
      UserIdentifier(tenantId, name)
    } else {
      logError(s"Failed to parse user identifier: $userIdentifier")
      throw new CelebornException(s"Failed to parse user identifier: ${userIdentifier}")
    }
  }
}
