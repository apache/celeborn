package org.apache.celeborn.client;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import org.apache.tez.runtime.library.api.IOInterruptedException;

import org.apache.celeborn.client.write.DataPusher;
import org.apache.celeborn.client.write.PushTask;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;

public class CelebornTezWriter {

  private final ShuffleClient shuffleClient;
  private DataPusher dataPusher;
  private long pushSortMemoryThreshold;
  private final int shuffleId;
  private final int mapId;
  private final int attemptNumber;
  private final int numMappers;
  private final int numPartitions;
  private final Consumer<Integer> afterPush;

  public CelebornTezWriter(
      int shuffleId,
      int mapId,
      int attemptNumber,
      long taskAttemptId,
      int numMappers,
      int numPartitions,
      CelebornConf conf,
      Consumer<Integer> afterPush,
      LongAdder[] mapStatusLengths,
      String appUniqueId,
      String lifecycleManagerHost,
      int lifecycleManagerPort,
      UserIdentifier userIdentifier) {
    shuffleClient =
        ShuffleClient.get(
            appUniqueId, lifecycleManagerHost, lifecycleManagerPort, conf, userIdentifier, null);
    // TEZ_SHUFFLE_ID
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptNumber = attemptNumber;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;

    this.afterPush = afterPush;
    try {
      LinkedBlockingQueue<PushTask> pushTaskQueue = new LinkedBlockingQueue<>();
      dataPusher =
          new DataPusher(
              shuffleId,
              mapId,
              attemptNumber,
              taskAttemptId,
              numMappers,
              numPartitions,
              conf,
              shuffleClient,
              pushTaskQueue,
              afterPush,
              mapStatusLengths);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void pushData(int partitionId, byte[] dataBuf) throws IOException {
    try {
      dataPusher.addTask(partitionId, dataBuf, dataBuf.length);
    } catch (InterruptedException e) {
      throw new IOInterruptedException(e);
    }
  }

  public void close() throws IOException {
    try {
      dataPusher.waitOnTermination();
      shuffleClient.mapperEnd(shuffleId, mapId, attemptNumber, numMappers);
    } catch (InterruptedException e) {
      throw new IOInterruptedException(e);
    }
  }
}
