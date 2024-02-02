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

package org.apache.celeborn.server.common.service.store.db;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;

public class DBSessionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DBSessionFactory.class);
  private static final String MYBATIS_CONFIG_PATH = "mybatis-config.xml";
  private static volatile SqlSessionFactory _instance;

  public static SqlSessionFactory get(CelebornConf celebornConf) throws IOException {
    if (_instance == null) {
      synchronized (DBSessionFactory.class) {
        if (_instance == null) {
          try (InputStream inputStream = Resources.getResourceAsStream(MYBATIS_CONFIG_PATH)) {
            Properties properties = new Properties();
            properties.setProperty(
                "driverClassName", celebornConf.dynamicConfigStoreDbDriverClassName());
            properties.setProperty("jdbcUrl", celebornConf.dynamicConfigStoreDbJdbcUrl());
            properties.setProperty("username", celebornConf.dynamicConfigStoreDbUsername());
            properties.setProperty("password", celebornConf.dynamicConfigStoreDbPassword());
            properties.setProperty(
                "connectionTimeout",
                String.valueOf(celebornConf.dynamicConfigStoreDbConnectionTimeout()));
            properties.setProperty(
                "idleTimeout", String.valueOf(celebornConf.dynamicConfigStoreDbIdleTimeout()));
            properties.setProperty(
                "maxLifetime", String.valueOf(celebornConf.dynamicConfigStoreDbMaxLifetime()));
            properties.setProperty(
                "maximumPoolSize",
                String.valueOf(celebornConf.dynamicConfigStoreDbMaximumPoolSize()));

            SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
            _instance = builder.build(inputStream, null, properties);
            LOG.info("Init sqlSessionFactory success");
          }
        }
      }
    }

    return _instance;
  }
}
