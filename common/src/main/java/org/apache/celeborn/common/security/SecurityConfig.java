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

package org.apache.celeborn.common.security;

public class SecurityConfig {
  private String krb5ConfPath;
  private String keytabFilePath;
  private String principal;
  private long reloginIntervalSec;
  private boolean enableProxyUser;

  private SecurityConfig() {
    // ignore.
  }

  public String getKrb5ConfPath() {
    return krb5ConfPath;
  }

  public String getKeytabFilePath() {
    return keytabFilePath;
  }

  public String getPrincipal() {
    return principal;
  }

  public long getReloginIntervalSec() {
    return reloginIntervalSec;
  }

  public boolean isEnableProxyUser() {
    return enableProxyUser;
  }

  public static class Builder {
    private SecurityConfig info;

    public Builder() {
      this.info = new SecurityConfig();
    }

    public Builder keytabFilePath(String keytabFilePath) {
      info.keytabFilePath = keytabFilePath;
      return this;
    }

    public Builder principal(String principal) {
      info.principal = principal;
      return this;
    }

    public Builder reloginIntervalSec(long reloginIntervalSec) {
      info.reloginIntervalSec = reloginIntervalSec;
      return this;
    }

    public Builder krb5ConfPath(String krb5ConfPath) {
      info.krb5ConfPath = krb5ConfPath;
      return this;
    }

    public Builder enableProxyUser(boolean enableProxyUser) {
      info.enableProxyUser = enableProxyUser;
      return this;
    }

    public SecurityConfig build() {
      return info;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
