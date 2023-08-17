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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle.ShuffleError;
import org.apache.hadoop.util.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.util.HadoopUtils;

public class CelebornShuffleConsumer<K, V>
    implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {
  private static Logger logger = LoggerFactory.getLogger(CelebornShuffleConsumer.class);
  private JobConf mrJobConf;
  private JobConf celebornJobConf;
  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private Context context;
  private org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
  private TaskUmbilicalProtocol umbilical;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private Task reduceTask;

  private String appId;
  private String lcHost;
  private int lcPort;
  private ShuffleClient shuffleClient;
  private CelebornConf celebornConf;

  @Override
  public void init(Context<K, V> context) {
    this.context = context;

    this.reduceId = context.getReduceId();
    this.mrJobConf = context.getJobConf();
    this.celebornJobConf = new JobConf(HadoopUtils.MR_CELEBORN_CONF);

    this.umbilical = context.getUmbilical();
    this.reporter = context.getReporter();
    this.metrics = createMetrics(reduceId, mrJobConf);
    this.copyPhase = context.getCopyPhase();
    this.taskStatus = context.getStatus();
    this.reduceTask = context.getReduceTask();

    this.appId = celebornJobConf.get(HadoopUtils.MR_CELEBORN_APPLICATION_ID);
    this.lcHost = celebornJobConf.get(HadoopUtils.MR_CELEBORN_LC_HOST);
    this.lcPort = Integer.parseInt(celebornJobConf.get(HadoopUtils.MR_CELEBORN_LC_PORT));
    celebornConf = HadoopUtils.fromYarnConf(mrJobConf);
    shuffleClient =
        ShuffleClient.get(
            appId,
            lcHost,
            lcPort,
            celebornConf,
            new UserIdentifier(
                celebornConf.quotaUserSpecificTenant(), celebornConf.quotaUserSpecificUserName()));
    this.merger = createMergeManager(context);
  }

  // Merge mapOutput and spill in local disks if necessary
  protected MergeManager<K, V> createMergeManager(ShuffleConsumerPlugin.Context context) {
    return new MergeManagerImpl<K, V>(
        reduceId,
        mrJobConf,
        context.getLocalFS(),
        context.getLocalDirAllocator(),
        reporter,
        context.getCodec(),
        context.getCombinerClass(),
        context.getCombineCollector(),
        context.getSpilledRecordsCounter(),
        context.getReduceCombineInputCounter(),
        context.getMergedMapOutputsCounter(),
        this,
        context.getMergePhase(),
        context.getMapOutputFile());
  }

  private ShuffleClientMetrics createMetrics(
      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID, JobConf jobConf) {
    return DynConstructors.builder()
        .impl("org.apache.hadoop.mapreduce.task.reduce.ShuffleClientMetrics")
        .build()
        .invoke(null, taskAttemptID, jobConf);
  }

  @Override
  public RawKeyValueIterator run() throws IOException, InterruptedException {
    logger.info("In reduce: " + reduceId + ", Celeborn mr client start to read shuffle data.");

    CelebornInputStream shuffleInputStream =
        shuffleClient.readPartition(
            0, reduceId.getTaskID().getId(), reduceId.getId(), 0, Integer.MAX_VALUE);
    CelebornShuffleReader<K, V> shuffleReader =
        new CelebornShuffleReader(
            reduceId, taskStatus, merger, copyPhase, reporter, metrics, shuffleInputStream);
    shuffleReader.fetchAndMerge();

    copyPhase.complete();
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);

    // Finish the on-going merges...
    RawKeyValueIterator kvIter = null;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge ", e);
    }

    logger.info("In reduce: " + reduceId + ", Celeborn mr client read shuffle data complete");

    return kvIter;
  }

  @Override
  public void close() {}

  @Override
  public void reportException(Throwable throwable) {
    if (this.throwable == null) {
      this.throwable = throwable;
    }
  }
}
