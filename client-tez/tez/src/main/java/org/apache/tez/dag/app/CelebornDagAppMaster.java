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

package org.apache.tez.dag.app;

import static org.apache.celeborn.tez.plugin.util.TezUtils.getPrivateField;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.Edge;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.state.OnStateChangedCallback;
import org.apache.tez.state.StateMachineTez;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.tez.plugin.util.TezUtils;

public class CelebornDagAppMaster extends DAGAppMaster {
  private static final Logger Logger = LoggerFactory.getLogger(CelebornDagAppMaster.class);
  private static final String MASTER_ENDPOINTS_ENV = "CELEBORN_MASTER_ENDPOINTS";

  private CelebornConf celebornConf;
  private LifecycleManager lifecycleManager;
  private ApplicationAttemptId appAttemptId;
  private String lifecycleManagerHost;
  private int lifecycleManagerPort;
  private AtomicInteger shuffleIdGenerator = new AtomicInteger(0);

  public CelebornDagAppMaster(
      ApplicationAttemptId applicationAttemptId,
      ContainerId containerId,
      String nmHost,
      int nmPort,
      int nmHttpPort,
      Clock clock,
      long appSubmitTime,
      boolean isSession,
      String workingDirectory,
      String[] localDirs,
      String[] logDirs,
      String clientVersion,
      Credentials credentials,
      String jobUserName,
      DAGProtos.AMPluginDescriptorProto pluginDescriptorProto) {
    super(
        applicationAttemptId,
        containerId,
        nmHost,
        nmPort,
        nmHttpPort,
        clock,
        appSubmitTime,
        isSession,
        workingDirectory,
        localDirs,
        logDirs,
        clientVersion,
        credentials,
        jobUserName,
        pluginDescriptorProto);
    appAttemptId = applicationAttemptId;
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);

    celebornConf = TezUtils.fromTezConfiguration(conf);
    lifecycleManager = new LifecycleManager(appAttemptId.toString(), celebornConf);
    lifecycleManagerHost = lifecycleManager.getHost();
    lifecycleManagerPort = lifecycleManager.getPort();
    Logger.info("Init Celeborn lifecycle manager");
  }

  private static void validateInputParam(String value, String param) throws IOException {
    if (value == null) {
      String msg = param + " is null";
      Logger.error(msg);
      throw new IOException(msg);
    }
  }

  @Override
  protected DAG createDAG(DAGProtos.DAGPlan dagPB) {
    DAG dag = super.createDAG(dagPB);

    List<Integer> currentDagShuffleIds = new ArrayList<>();

    StateMachineTez stateMachine = (StateMachineTez) getPrivateField(dag, "stateMachine");
    stateMachine.registerStateEnteredCallback(
        DAGState.INITED,
        (OnStateChangedCallback<DAGState, DAGImpl>)
            (tmpDag, dagState) -> {
              try {
                Map<String, Edge> edges = (Map<String, Edge>) getPrivateField(tmpDag, "edges");
                for (Map.Entry<String, Edge> entry : edges.entrySet()) {
                  Edge edge = entry.getValue();

                  Configuration edgeSourceConf =
                      org.apache.tez.common.TezUtils.createConfFromUserPayload(
                          edge.getEdgeProperty().getEdgeSource().getUserPayload());
                  int shuffleId = shuffleIdGenerator.getAndIncrement();
                  currentDagShuffleIds.add(shuffleId);
                  edgeSourceConf.setInt(TezUtils.TEZ_SHUFFLE_ID, shuffleId);
                  edgeSourceConf.set(TezUtils.TEZ_CELEBORN_APPLICATION_ID, appAttemptId.toString());
                  edgeSourceConf.set(TezUtils.TEZ_CELEBORN_LM_HOST, lifecycleManagerHost);
                  edgeSourceConf.setInt(TezUtils.TEZ_CELEBORN_LM_PORT, lifecycleManagerPort);
                  for (Tuple2<String, String> stringStringTuple2 : celebornConf.getAll()) {
                    edgeSourceConf.set(stringStringTuple2._1, stringStringTuple2._2);
                  }

                  edge.getEdgeProperty()
                      .getEdgeSource()
                      .setUserPayload(
                          org.apache.tez.common.TezUtils.createUserPayloadFromConf(edgeSourceConf));
                  edge.getEdgeProperty()
                      .getEdgeDestination()
                      .setUserPayload(
                          org.apache.tez.common.TezUtils.createUserPayloadFromConf(edgeSourceConf));

                  EdgeProperty.DataMovementType dataMovementType =
                      edge.getEdgeProperty().getDataMovementType();

                  // rename output class name
                  OutputDescriptor outputDescriptor = edge.getEdgeProperty().getEdgeSource();
                  Field outputClassNameField =
                      outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
                  outputClassNameField.setAccessible(true);
                  String outputClassName = (String) outputClassNameField.get(outputDescriptor);
                  outputClassNameField.set(
                      outputDescriptor, getNewOutputClassName(dataMovementType, outputClassName));

                  // rename input class name
                  InputDescriptor inputDescriptor = edge.getEdgeProperty().getEdgeDestination();
                  Field inputClassNameField =
                      outputDescriptor.getClass().getSuperclass().getDeclaredField("className");
                  inputClassNameField.setAccessible(true);
                  String inputClassName = (String) outputClassNameField.get(inputDescriptor);
                  outputClassNameField.set(
                      inputDescriptor, getNewInputClassName(dataMovementType, inputClassName));
                }
              } catch (IOException | IllegalAccessException | NoSuchFieldException e) {
                Logger.error("Reconfigure failed after dag was inited, caused by {}", e);
                throw new TezUncheckedException(e);
              }
            });

    // process dag final status
    List<DAGState> finalStates =
        Arrays.asList(DAGState.SUCCEEDED, DAGState.FAILED, DAGState.KILLED, DAGState.ERROR);
    Map callbackMap = (Map) getPrivateField(stateMachine, "callbackMap");
    finalStates.forEach(
        finalState ->
            callbackMap.put(
                finalState,
                (OnStateChangedCallback<DAGState, DAGImpl>)
                    (tmpDag, dagState) -> {
                      // clean all shuffle for this Dag
                      for (Integer shuffleId : currentDagShuffleIds) {
                        lifecycleManager.unregisterShuffle(shuffleId);
                      }
                    }));
    return dag;
  }

  private static String getNewOutputClassName(
      EdgeProperty.DataMovementType movementType, String oldClassName) {
    // TODO: Implement real logic here
    return "";
  }

  private static String getNewInputClassName(
      EdgeProperty.DataMovementType movementType, String oldClassName) {
    // TODO: Implement real logic here
    return "";
  }

  @Override
  public void serviceStop() throws Exception {
    lifecycleManager.stop();
    super.serviceStop();
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      final String pid = System.getenv().get("JVM_PID");
      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
      String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr = System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      validateInputParam(appSubmitTimeStr, ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());

      // Command line options
      Options opts = new Options();
      opts.addOption(
          TezConstants.TEZ_SESSION_MODE_CLI_OPTION,
          false,
          "Run Tez Application Master in Session mode");

      CommandLine cliParser = new GnuParser().parse(opts, args);
      boolean sessionModeCliOption = cliParser.hasOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION);

      Logger.info(
          "Creating CelebornDAGAppMaster for "
              + "applicationId="
              + applicationAttemptId.getApplicationId()
              + ", attemptNum="
              + applicationAttemptId.getAttemptId()
              + ", AMContainerId="
              + containerId
              + ", jvmPid="
              + pid
              + ", userFromEnv="
              + jobUserName
              + ", cliSessionOption="
              + sessionModeCliOption
              + ", pwd="
              + System.getenv(ApplicationConstants.Environment.PWD.name())
              + ", localDirs="
              + System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())
              + ", logDirs="
              + System.getenv(ApplicationConstants.Environment.LOG_DIRS.name()));

      // disable tez slow start
      Configuration conf = new Configuration(new YarnConfiguration());
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, 1.0f);
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, 1.0f);
      // disable reschedule task on unhealthy nodes because shuffle data are stored in Celeborn
      conf.setBoolean(TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS, false);

      // support set celeborn master endpoints from env
      String masterEndpointsKey = TezUtils.TEZ_PREFIX + CelebornConf.MASTER_ENDPOINTS().key();
      String masterEndpointsVal = conf.get(masterEndpointsKey);
      if (masterEndpointsVal == null || masterEndpointsVal.isEmpty()) {
        Logger.info(
            "MRAppMaster sets {} via environment variable {}.",
            masterEndpointsKey,
            MASTER_ENDPOINTS_ENV);
        conf.set(masterEndpointsKey, TezUtils.ensureGetSysEnv(MASTER_ENDPOINTS_ENV));
      }

      DAGProtos.ConfigurationProto confProto =
          TezUtilsInternal.readUserSpecifiedTezConfiguration(
              System.getenv(ApplicationConstants.Environment.PWD.name()));
      TezUtilsInternal.addUserSpecifiedTezConfiguration(conf, confProto.getConfKeyValuesList());

      DAGProtos.AMPluginDescriptorProto amPluginDescriptorProto = null;
      if (confProto.hasAmPluginDescriptor()) {
        amPluginDescriptorProto = confProto.getAmPluginDescriptor();
      }

      UserGroupInformation.setConfiguration(conf);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

      TezUtilsInternal.setSecurityUtilConfigration(Logger, conf);

      CelebornDagAppMaster appMaster =
          new CelebornDagAppMaster(
              applicationAttemptId,
              containerId,
              nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString),
              new SystemClock(),
              appSubmitTime,
              sessionModeCliOption,
              System.getenv(ApplicationConstants.Environment.PWD.name()),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.name())),
              TezCommonUtils.getTrimmedStrings(
                  System.getenv(ApplicationConstants.Environment.LOG_DIRS.name())),
              clientVersion,
              credentials,
              jobUserName,
              amPluginDescriptorProto);
      ShutdownHookManager.get()
          .addShutdownHook(new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

      // log the system properties
      if (Logger.isInfoEnabled()) {
        String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(conf);
        if (systemPropsToLog != null) {
          Logger.info(systemPropsToLog);
        }
      }

      initAndStartAppMaster(appMaster, conf);

    } catch (Throwable t) {
      Logger.error("Error starting DAGAppMaster", t);
      System.exit(1);
    }
  }
}
