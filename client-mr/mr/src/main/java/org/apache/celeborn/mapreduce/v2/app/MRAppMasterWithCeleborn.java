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

package org.apache.celeborn.mapreduce.v2.app;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.util.HadoopUtils;

public class MRAppMasterWithCeleborn extends MRAppMaster {
  private static final Logger logger = LoggerFactory.getLogger(MRAppMasterWithCeleborn.class);

  private static final String MASTER_ENDPOINTS_ENV = "CELEBORN_MASTER_ENDPOINTS";

  public MRAppMasterWithCeleborn(
      ApplicationAttemptId applicationAttemptId,
      ContainerId containerId,
      String nmHost,
      int nmPort,
      int nmHttpPort,
      long appSubmitTime,
      JobConf jobConf)
      throws CelebornIOException {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, appSubmitTime);

    int numReducers = jobConf.getInt(MRJobConfig.NUM_REDUCES, 0);
    if (numReducers > 0) {
      CelebornConf conf = HadoopUtils.fromYarnConf(jobConf);
      String appUniqueId = conf.appUniqueIdWithUUIDSuffix(applicationAttemptId.toString());
      LifecycleManager lifecycleManager = new LifecycleManager(appUniqueId, conf);
      String lmHost = lifecycleManager.getHost();
      int lmPort = lifecycleManager.getPort();
      logger.info("MRAppMaster initialized with {} {} {}", lmHost, lmPort, appUniqueId);
      JobConf lmConf = new JobConf();
      lmConf.clear();
      lmConf.set(HadoopUtils.MR_CELEBORN_LM_HOST, lmHost);
      lmConf.set(HadoopUtils.MR_CELEBORN_LM_PORT, lmPort + "");
      lmConf.set(HadoopUtils.MR_CELEBORN_APPLICATION_ID, appUniqueId);
      writeLifecycleManagerConfToTask(jobConf, lmConf);
    }
  }

  private void writeLifecycleManagerConfToTask(JobConf conf, JobConf lmConf)
      throws CelebornIOException {
    try {
      String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
      Path celebornConfPath = new Path(jobDirStr, HadoopUtils.MR_CELEBORN_CONF);
      FileSystem fs = celebornConfPath.getFileSystem(conf);

      try (FSDataOutputStream out =
          FileSystem.create(
              fs, celebornConfPath, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
        lmConf.writeXml(out);
      }
      FileStatus status = fs.getFileStatus(celebornConfPath);
      long currentTs = status.getModificationTime();
      String uri =
          celebornConfPath.toUri().isAbsolute()
              ? celebornConfPath.toUri().toString()
              : fs.getUri() + Path.SEPARATOR + celebornConfPath.toUri();
      String files = conf.get(MRJobConfig.CACHE_FILES);
      conf.set(MRJobConfig.CACHE_FILES, files == null ? uri : uri + "," + files);
      String ts = conf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS);
      conf.set(
          MRJobConfig.CACHE_FILE_TIMESTAMPS,
          ts == null ? String.valueOf(currentTs) : currentTs + "," + ts);
      String vis = conf.get(MRJobConfig.CACHE_FILE_VISIBILITIES);
      conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, vis == null ? "false" : "false" + "," + vis);
      long size = status.getLen();
      String sizes = conf.get(MRJobConfig.CACHE_FILES_SIZES);
      conf.set(
          MRJobConfig.CACHE_FILES_SIZES, sizes == null ? String.valueOf(size) : size + "," + sizes);
    } catch (IOException e) {
      logger.error("Upload extra conf exception", e);
      throw new CelebornIOException("Upload extra conf exception ", e);
    }
  }

  private static String ensureGetSysEnv(String envName) throws IOException {
    String value = System.getenv(envName);
    if (value == null) {
      String msg = envName + " is null";
      logger.error(msg);
      throw new CelebornIOException(msg);
    }
    return value;
  }

  public static void main(String[] args) {
    JobConf rmAppConf = new JobConf(new YarnConfiguration());
    rmAppConf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    try {
      checkJobConf(rmAppConf);
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr = ensureGetSysEnv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String nodeHostString = ensureGetSysEnv(ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = ensureGetSysEnv(ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
          ensureGetSysEnv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr = ensureGetSysEnv("APP_SUBMIT_TIME_ENV");
      ContainerId containerId = ContainerId.fromString(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
      if (applicationAttemptId != null) {
        CallerContext.setCurrent(
            new CallerContext.Builder("mr_app_master_with_celeborn_" + applicationAttemptId)
                .build());
      }

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      MRAppMasterWithCeleborn appMaster =
          new MRAppMasterWithCeleborn(
              applicationAttemptId,
              containerId,
              nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString),
              appSubmitTime,
              rmAppConf);

      // set this flag to avoid exit exception
      try {
        Field field = MRAppMaster.class.getDeclaredField("mainStarted");
        field.setAccessible(true);
        field.setBoolean(null, true);
        field.setAccessible(false);
      } catch (NoSuchFieldException e) {
        // ignore it for compatibility
      }

      ShutdownHookManager.get()
          .addShutdownHook(
              () -> {
                logger.info("MRAppMasterWithCeleborn received stop signal.");
                appMaster.notifyIsLastAMRetry(appMaster.isLastAMRetry);
                appMaster.stop();
              },
              30);
      MRWebAppUtil.initialize(rmAppConf);
      String systemPropsToLog = MRApps.getSystemPropertiesToLog(rmAppConf);
      if (systemPropsToLog != null) {
        logger.info(systemPropsToLog);
      }
      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
      rmAppConf.set("mapreduce.job.user.name", jobUserName);
      initAndStartAppMaster(appMaster, rmAppConf, jobUserName);
    } catch (Throwable t) {
      logger.error("Error starting MRAppMaster", t);
      ExitUtil.terminate(1, t);
    }
  }

  public static void checkJobConf(JobConf conf) throws IOException {
    if (conf.getBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false)) {
      logger.warn("MRAppMaster disables job recovery.");
      // MapReduce does not set the flag which indicates whether to keep containers across
      // application attempts in ApplicationSubmissionContext. Therefore, there is no container
      // shared between attempts.
      conf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false);
    }
    if (conf.getFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.05f) != 1.0f) {
      logger.warn("MRAppMaster disables job reduce slow start.");
      // Make sure reduces are scheduled only after all map are completed.
      conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);
    }
    String masterEndpointsKey = HadoopUtils.MR_PREFIX + CelebornConf.MASTER_ENDPOINTS().key();
    String masterEndpointsVal = conf.get(masterEndpointsKey);
    if (masterEndpointsVal == null || masterEndpointsVal.isEmpty()) {
      logger.info(
          "MRAppMaster sets {} via environment variable {}.",
          masterEndpointsKey,
          MASTER_ENDPOINTS_ENV);
      conf.set(masterEndpointsKey, ensureGetSysEnv(MASTER_ENDPOINTS_ENV));
    }
  }
}
