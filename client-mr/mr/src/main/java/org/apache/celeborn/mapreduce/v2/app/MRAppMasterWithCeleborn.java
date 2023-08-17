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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
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
  private static Logger logger = LoggerFactory.getLogger(MRAppMasterWithCeleborn.class);
  private LifecycleManager lifecycleManager;

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
      lifecycleManager = new LifecycleManager(applicationAttemptId.toString(), conf);
      String lcHost = lifecycleManager.getHost();
      int lcPort = lifecycleManager.getPort();
      logger.info("RMAppMaster initialized with {} {} {}", lcHost, lcPort, applicationAttemptId);
      JobConf lcConf = new JobConf();
      lcConf.clear();
      lcConf.set(HadoopUtils.MR_CELEBORN_LC_HOST, lcHost);
      lcConf.set(HadoopUtils.MR_CELEBORN_LC_PORT, lcPort + "");
      lcConf.set(HadoopUtils.MR_CELEBORN_APPLICATION_ID, applicationAttemptId.toString());
      writeLifecycleManagerConfToTask(jobConf, lcConf);
    }
  }

  void writeLifecycleManagerConfToTask(JobConf conf, JobConf lcConf) throws CelebornIOException {
    try {
      FileSystem fs = new Cluster(conf).getFileSystem();
      String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
      Path celebornExtraConf = new Path(jobDirStr, HadoopUtils.MR_CELEBORN_CONF);

      try (FSDataOutputStream out =
          FileSystem.create(
              fs, celebornExtraConf, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
        lcConf.writeXml(out);
      }
      FileStatus status = fs.getFileStatus(celebornExtraConf);
      long currentTs = status.getModificationTime();
      String uri = fs.getUri() + Path.SEPARATOR + celebornExtraConf.toUri();
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
    } catch (InterruptedException | IOException e) {
      logger.error("Upload extra conf exception", e);
      throw new CelebornIOException("Upload extra conf exception ", e);
    }
  }

  private static void validateInputParam(String value, String param) throws IOException {
    if (value == null) {
      String msg = param + " is null";
      logger.error(msg);
      throw new IOException(msg);
    }
  }

  public static void main(String[] args) {
    JobConf rmAppConf = new JobConf(new YarnConfiguration());
    rmAppConf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      validateInputParam(containerIdStr, ApplicationConstants.Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
      validateInputParam(nodeHostString, ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
      validateInputParam(nodePortString, ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      validateInputParam(nodeHttpPortString, ApplicationConstants.Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr = System.getenv("APP_SUBMIT_TIME_ENV");
      validateInputParam(appSubmitTimeStr, "APP_SUBMIT_TIME_ENV");
      ContainerId containerId = ContainerId.fromString(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
      if (applicationAttemptId != null) {
        CallerContext.setCurrent(
            (new CallerContext.Builder(
                    "mr_app_master_with_celeborn_" + applicationAttemptId.toString()))
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
}
