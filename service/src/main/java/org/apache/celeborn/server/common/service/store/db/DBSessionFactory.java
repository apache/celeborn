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
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterInfoMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterSystemConfigMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterTagsMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterTenantConfigMapper;

public class DBSessionFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DBSessionFactory.class);
  private static volatile SqlSessionFactory _instance;

  public static SqlSessionFactory get(CelebornConf celebornConf) throws IOException {
    if (_instance == null) {
      synchronized (DBSessionFactory.class) {
        if (_instance == null) {
          Properties properties = new Properties();
          properties.setProperty(
              "driverClassName", celebornConf.dynamicConfigStoreDbHikariDriverClassName());
          properties.setProperty("jdbcUrl", celebornConf.dynamicConfigStoreDbHikariJdbcUrl());
          properties.setProperty("username", celebornConf.dynamicConfigStoreDbHikariUsername());
          properties.setProperty("password", celebornConf.dynamicConfigStoreDbHikariPassword());
          properties.setProperty(
              "connectionTimeout",
              String.valueOf(celebornConf.dynamicConfigStoreDbHikariConnectionTimeout()));
          properties.setProperty(
              "idleTimeout", String.valueOf(celebornConf.dynamicConfigStoreDbHikariIdleTimeout()));
          properties.setProperty(
              "maxLifetime", String.valueOf(celebornConf.dynamicConfigStoreDbHikariMaxLifetime()));
          properties.setProperty(
              "maximumPoolSize",
              String.valueOf(celebornConf.dynamicConfigStoreDbHikariMaximumPoolSize()));

          for (Map.Entry<String, String> dbPropertiesEntry :
              celebornConf.dynamicConfigStoreDbHikariCustomConfigs().entrySet()) {
            properties.setProperty(
                dbPropertiesEntry.getKey().replace("celeborn.dynamicConfig.store.db.hikari.", ""),
                dbPropertiesEntry.getValue());
          }

          HikariConfig config = new HikariConfig(properties);
          DataSource dataSource = new HikariDataSource(config);

          TransactionFactory transactionFactory = new JdbcTransactionFactory();
          Environment environment = new Environment("celeborn", transactionFactory, dataSource);

          Configuration configuration = new Configuration(environment);
          configuration.setMapUnderscoreToCamelCase(true);
          configuration.addMapper(ClusterInfoMapper.class);
          configuration.addMapper(ClusterSystemConfigMapper.class);
          configuration.addMapper(ClusterTenantConfigMapper.class);
          configuration.addMapper(ClusterTagsMapper.class);

          SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
          _instance = builder.build(configuration);
          LOG.info("Init sqlSessionFactory success");
        }
      }
    }

    return _instance;
  }
}
