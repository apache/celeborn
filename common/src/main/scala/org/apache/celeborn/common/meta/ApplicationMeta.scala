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

package org.apache.celeborn.common.meta

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.ApplicationMeta.UNKNOWN_USER_IDENTIFIER

/**
 * Application meta
 */
case class ApplicationMeta(
    appId: String,
    secret: String,
    var userIdentifier: UserIdentifier = UNKNOWN_USER_IDENTIFIER,
    var extraInfo: JMap[String, String] = Map.empty[String, String].asJava,
    registrationTime: Long = System.currentTimeMillis()) {
  def this(appId: String, secret: String) = {
    this(
      appId,
      secret,
      UNKNOWN_USER_IDENTIFIER,
      Map.empty[String, String].asJava,
      System.currentTimeMillis())
  }

  def this(appId: String, userIdentifier: UserIdentifier, extraInfo: JMap[String, String]) = {
    this(appId, null, userIdentifier, extraInfo, System.currentTimeMillis())
  }
}

object ApplicationMeta {
  val UNKNOWN_USER_IDENTIFIER = UserIdentifier("unknown", "unknown")
}
