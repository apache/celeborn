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

package org.apache.celeborn.server.common.http

import java.net.URL
import java.util.Locale

object HttpUtils {
  def parseUrl(uri: String): (String, Map[String, String]) = {
    val url = new URL(s"https://127.0.0.1:9000$uri")
    val parameter =
      if (url.getQuery == null) {
        Map.empty[String, String]
      } else {
        url.getQuery
          .split("&")
          .map(_.split("="))
          .map(arr => arr(0).toUpperCase(Locale.ROOT) -> arr(1).toUpperCase(Locale.ROOT)).toMap
      }
    (url.getPath, parameter)
  }
}
