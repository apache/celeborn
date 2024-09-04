/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.tests.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.Preconditions;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.output.CelebornUnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BroadcastAndOneToOneExample extends Configured implements Tool {
  public static class InputProcessor extends SimpleProcessor {
    Text word = new Text();

    public InputProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getOutputs().size() == 1);
      CelebornUnorderedKVOutput output = (CelebornUnorderedKVOutput) getOutputs().values().iterator().next();
      KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
      kvWriter.write(word, new IntWritable(getContext().getTaskIndex()));
      ByteBuffer userPayload =
          getContext().getUserPayload() == null ? null : getContext().getUserPayload().getPayload();
      if (userPayload != null) {
        boolean doLocalityCheck = getContext().getUserPayload().getPayload().get(0) > 0 ? true : false;
        if (doLocalityCheck) {
          ObjectRegistry objectRegistry = getContext().getObjectRegistry();
          String entry = String.valueOf(getContext().getTaskIndex());
          objectRegistry.cacheForDAG(entry, entry);
        }
      }
    }
  }

  public static class OneToOneProcessor extends SimpleProcessor {
    Text word = new Text();

    public OneToOneProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(inputs.size() == 2);

      KeyValueReader inputKvReader = (KeyValueReader) getInputs().get("Input").getReader();
      KeyValueReader broadcastKvReader = (KeyValueReader) getInputs().get("Broadcast").getReader();
      int sum = 0;
      while (broadcastKvReader.next()) {
        sum += ((IntWritable) broadcastKvReader.getCurrentValue()).get();
      }
      while (inputKvReader.next()) {
        sum += ((IntWritable) inputKvReader.getCurrentValue()).get();
      }
      boolean doLocalityCheck = getContext().getUserPayload().getPayload().get(0) > 0 ? true : false;
      int broadcastSum = getContext().getUserPayload().getPayload().get(1);
      int expectedSum = broadcastSum + getContext().getTaskIndex();
      System.out.println("Index: " + getContext().getTaskIndex() +
          " sum: " + sum + " expectedSum: " + expectedSum + " broadcastSum: " + broadcastSum);
      Preconditions.checkState((sum == expectedSum), "Sum = " + sum);

      if (doLocalityCheck) {
        ObjectRegistry objectRegistry = getContext().getObjectRegistry();
        String index = (String) objectRegistry.get(String.valueOf(getContext().getTaskIndex()));
        if (index == null || Integer.parseInt(index) != getContext().getTaskIndex()) {
          String msg = "Did not find expected local producer "
              + getContext().getTaskIndex() + " in the same JVM";
          System.out.println(msg);
          throw new TezUncheckedException(msg);
        }
      }
    }

  }

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
                        Path stagingDir, boolean doLocalityCheck) throws IOException, YarnException {

    int numBroadcastTasks = 2;
    int numOneToOneTasks = 3;
    if (doLocalityCheck) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(tezConf);
      yarnClient.start();
      int numNMs = yarnClient.getNodeReports(NodeState.RUNNING).size();
      yarnClient.stop();
      // create enough 1-1 tasks to run in parallel
      numOneToOneTasks = numNMs - numBroadcastTasks - 1;// 1 AM
      if (numOneToOneTasks < 1) {
        numOneToOneTasks = 1;
      }
    }
    byte[] procByte = {(byte) (doLocalityCheck ? 1 : 0), 1};
    UserPayload procPayload = UserPayload.create(ByteBuffer.wrap(procByte));

    System.out.println("Using " + numOneToOneTasks + " 1-1 tasks");

    Vertex broadcastVertex = Vertex.create("Broadcast", ProcessorDescriptor.create(
        InputProcessor.class.getName()), numBroadcastTasks);

    Vertex inputVertex = Vertex.create("Input", ProcessorDescriptor.create(
        InputProcessor.class.getName()).setUserPayload(procPayload), numOneToOneTasks);

    Vertex oneToOneVertex = Vertex.create("OneToOne",
        ProcessorDescriptor.create(
            OneToOneProcessor.class.getName()).setUserPayload(procPayload));
    oneToOneVertex.setVertexManagerPlugin(
        VertexManagerPluginDescriptor.create(InputReadyVertexManager.class.getName()));

    UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName())
        .setFromConfiguration(tezConf).build();

    DAG dag = DAG.create("BroadcastAndOneToOneExample");
    dag.addVertex(inputVertex)
        .addVertex(broadcastVertex)
        .addVertex(oneToOneVertex)
        .addEdge(
            Edge.create(inputVertex, oneToOneVertex, edgeConf.createDefaultOneToOneEdgeProperty()))
        .addEdge(
            Edge.create(broadcastVertex, oneToOneVertex,
                edgeConf.createDefaultBroadcastEdgeProperty()));
    return dag;
  }

  public boolean run(Configuration conf, boolean doLocalityCheck) throws Exception {
    System.out.println("Running BroadcastAndOneToOneExample");
    // conf and UGI
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    UserGroupInformation.setConfiguration(tezConf);

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String stagingDirStr = tezConf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
        TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT) + Path.SEPARATOR +
        "BroadcastAndOneToOneExample" + Path.SEPARATOR + Long.toString(System.currentTimeMillis());
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    TezClient tezSession = null;
    // needs session or else TaskScheduler does not hold onto containers
    tezSession = TezClient.create("broadcastAndOneToOneExample", tezConf);
    tezSession.start();

    DAGClient dagClient = null;

    try {
      DAG dag = createDAG(fs, tezConf, stagingDir, doLocalityCheck);

      tezSession.waitTillReady();
      dagClient = tezSession.submitDAG(dag);

      // monitoring
      DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
        return false;
      }
      return true;
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    boolean doLocalityCheck = true;
    if (args.length == 1) {
      if (args[0].equals(skipLocalityCheck)) {
        doLocalityCheck = false;
      } else {
        printUsage();
        throw new TezException("Invalid command line");
      }
    } else if (args.length > 1) {
      printUsage();
      throw new TezException("Invalid command line");
    }

    if (doLocalityCheck &&
        getConf().getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
            TezConfiguration.TEZ_LOCAL_MODE_DEFAULT)) {
      System.out.println("locality check is not valid in local mode. skipping");
      doLocalityCheck = false;
    }

    boolean status = run(getConf(), doLocalityCheck);
    return status ? 0 : 1;
  }

  private static void printUsage() {
    System.err.println("broadcastAndOneToOneExample " + skipLocalityCheck);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  static String skipLocalityCheck = "-skipLocalityCheck";

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    BroadcastAndOneToOneExample job = new BroadcastAndOneToOneExample();
    int status = ToolRunner.run(conf, job, args);
    System.exit(status);
  }
}
