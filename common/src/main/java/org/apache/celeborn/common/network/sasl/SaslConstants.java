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

package org.apache.celeborn.common.network.sasl;

import java.util.Map;

import javax.security.sasl.Sasl;

import com.google.common.collect.ImmutableMap;

public interface SaslConstants {
  /** Sasl Mechanisms */
  String ANONYMOUS = "ANONYMOUS";

  String DIGEST = "DIGEST-MD5";

  /** Quality of protection value that includes encryption. */
  String QOP_AUTH_CONF = "auth-conf";
  /** Quality of protection value that does not include encryption. */
  String QOP_AUTH = "auth";

  String DEFAULT_REALM = "default";

  Map<String, String> DEFAULT_SASL_CLIENT_PROPS =
      ImmutableMap.<String, String>builder().put(Sasl.QOP, QOP_AUTH).build();

  Map<String, String> DEFAULT_SASL_SERVER_PROPS =
      ImmutableMap.<String, String>builder()
          .put(Sasl.SERVER_AUTH, "true")
          .put(Sasl.QOP, String.format("%s,%s", QOP_AUTH_CONF, QOP_AUTH))
          .build();
}
