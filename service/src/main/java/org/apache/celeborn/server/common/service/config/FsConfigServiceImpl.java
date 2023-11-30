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

package org.apache.celeborn.server.common.service.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import org.apache.celeborn.common.CelebornConf;

public class FsConfigServiceImpl extends BaseConfigServiceImpl {
  private static final Logger LOG = LoggerFactory.getLogger(FsConfigServiceImpl.class);

  private File configurationFile;

  public FsConfigServiceImpl(CelebornConf celebornConf) {
    super(celebornConf);
  }

  protected void init() {
    loadConfigurationFile();
    if (!configurationFile.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "Dynamic config file does not exist: %s.", configurationFile.getAbsolutePath()));
    }
  }

  protected synchronized void refresh() {
    loadConfigurationFile();
    if (!configurationFile.exists()) {
      return;
    }

    List<Map<String, Object>> dynamicConfigs;
    try (FileInputStream fileInputStream = new FileInputStream(configurationFile)) {
      Yaml yaml = new Yaml();
      dynamicConfigs = yaml.load(fileInputStream);
    } catch (Exception e) {
      LOG.error(
          "Refresh dynamic config of {} error: {}.",
          configurationFile.getAbsolutePath(),
          e.getMessage(),
          e);
      return;
    }
    refreshConfig(dynamicConfigs);
  }

  private void loadConfigurationFile() {
    Option<String> configurationPathOpt = celebornConf.quotaConfigurationPath();
    configurationFile =
        new File(
            configurationPathOpt.isEmpty()
                ? defaultQuotaConfigurationPath()
                : configurationPathOpt.get());
  }

  private String defaultQuotaConfigurationPath() {
    Map<String, String> env = System.getenv();
    return Optional.ofNullable(env.get("CELEBORN_CONF_DIR"))
            .orElse(env.getOrDefault("CELEBORN_HOME", ".") + File.separator + "conf")
        + File.separator
        + "dynamicConfig.yaml";
  }
}
