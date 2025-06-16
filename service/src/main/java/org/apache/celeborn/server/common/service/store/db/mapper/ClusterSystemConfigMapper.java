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

package org.apache.celeborn.server.common.service.store.db.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import org.apache.celeborn.server.common.service.model.ClusterSystemConfig;

public interface ClusterSystemConfigMapper {

  @Insert(
      "INSERT INTO celeborn_cluster_system_config(cluster_id, config_key, config_value, gmt_create, gmt_modify) "
          + "VALUES (#{clusterId}, #{configKey}, #{configValue}, #{gmtCreate}, #{gmtModify})")
  void insert(ClusterSystemConfig clusterSystemConfig);

  @Update(
      "UPDATE celeborn_cluster_system_config SET config_value = #{configValue}, gmt_modify = #{gmtModify} "
          + "WHERE cluster_id = #{clusterId} AND config_key = #{configKey}")
  int update(ClusterSystemConfig clusterSystemConfig);

  @Delete(
      "DELETE FROM celeborn_cluster_system_config "
          + "WHERE cluster_id = #{clusterId} AND config_key = #{configKey}")
  int delete(ClusterSystemConfig clusterSystemConfig);

  @Select(
      "SELECT id, cluster_id, config_key, config_value, type, gmt_create, gmt_modify "
          + "FROM celeborn_cluster_system_config WHERE cluster_id = #{clusterId}")
  List<ClusterSystemConfig> getClusterSystemConfig(int clusterId);
}
