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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.util.HadoopUtils;

public class CelebornMapOutputCollector<K extends Object, V extends Object>
    implements MapOutputCollector<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(CelebornMapOutputCollector.class);
  private Class<K> keyClass;
  private Class<V> valClass;
  private Task.TaskReporter reporter;
  private CelebornSortBasedPusher<K, V> celebornSortBasedPusher;
  private int numReducers;

  @Override
  public void init(Context context) throws IOException {
    JobConf jobConf = context.getJobConf();
    reporter = context.getReporter();
    keyClass = (Class<K>) jobConf.getMapOutputKeyClass();
    valClass = (Class<V>) jobConf.getMapOutputValueClass();
    context.getMapTask().getTaskID().getId();
    numReducers = jobConf.getNumReduceTasks();

    int IOBufferSize = jobConf.getInt(JobContext.IO_SORT_MB, 100);
    // Java bytebuffer cannot be larger than Integer.MAX_VALUE
    if ((IOBufferSize & 0x7FF) != IOBufferSize) {
      throw new IOException("Invalid \"" + JobContext.IO_SORT_MB + "\": " + IOBufferSize);
    }

    CelebornConf celebornConf = HadoopUtils.fromYarnConf(jobConf);
    JobConf celebornAppendConf = new JobConf(HadoopUtils.MR_CELEBORN_CONF);
    String lmHost = celebornAppendConf.get(HadoopUtils.MR_CELEBORN_LM_HOST);
    int lmPort = Integer.parseInt(celebornAppendConf.get(HadoopUtils.MR_CELEBORN_LM_PORT));
    String applicationAttemptId = celebornAppendConf.get(HadoopUtils.MR_CELEBORN_APPLICATION_ID);
    logger.info(
        "Mapper initialized with celeborn {} {} {} {}",
        lmHost,
        lmPort,
        applicationAttemptId,
        IOBufferSize);
    UserIdentifier userIdentifier =
        new UserIdentifier(
            celebornConf.quotaUserSpecificTenant(), celebornConf.quotaUserSpecificUserName());

    final float spiller = jobConf.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float) 0.8);
    int pushSize = (int) ((IOBufferSize << 20) * spiller);

    SerializationFactory serializationFactory = new SerializationFactory(jobConf);
    celebornSortBasedPusher =
        new CelebornSortBasedPusher<>(
            jobConf.getNumMapTasks(),
            jobConf.getNumReduceTasks(),
            // this is map id
            context.getMapTask().getTaskID().getTaskID().getId(),
            // this is attempt id
            context.getMapTask().getTaskID().getId(),
            serializationFactory.getSerializer(keyClass),
            serializationFactory.getSerializer(valClass),
            IOBufferSize << 20,
            pushSize,
            jobConf.getOutputKeyComparator(),
            reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES),
            reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS),
            ShuffleClient.get(applicationAttemptId, lmHost, lmPort, celebornConf, userIdentifier),
            celebornConf);
  }

  @Override
  public void collect(K key, V value, int partition) throws IOException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException(
          "Type mismatch in key from map: expected "
              + keyClass.getName()
              + ", received "
              + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException(
          "Type mismatch in value from map: expected "
              + valClass.getName()
              + ", received "
              + value.getClass().getName());
    }
    if (partition < 0 || partition >= numReducers) {
      throw new IOException("Illegal partition for " + key + " (" + partition + ")");
    }
    celebornSortBasedPusher.checkException();
    celebornSortBasedPusher.insert(key, value, partition);
  }

  @Override
  public void close() {
    logger.info("Mapper collector close");
    reporter.progress();
    celebornSortBasedPusher.close();
  }

  @Override
  public void flush() {
    logger.info("Mapper collector flush");
    celebornSortBasedPusher.flush();
    reporter.progress();
  }
}
