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

package org.apache.celeborn.tests.tez;

import static org.apache.celeborn.common.util.Utils.selectRandomPort;

import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.service.deploy.master.Master;
import org.apache.celeborn.service.deploy.master.MasterArguments;
import org.apache.celeborn.service.deploy.worker.Worker;
import org.apache.celeborn.service.deploy.worker.WorkerArguments;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

public class MiniCelebornCluster {
  public static Master master;
  public static Thread masterThread;
  public static Map<Worker, Thread> workerInfos = new HashMap<>();
  private static Logger Logger = LoggerFactory.getLogger(MiniCelebornCluster.class);

  public static void setupMiniClusterWithRandomPorts() throws BindException, InterruptedException {
    int retryCount = 0;
    boolean created = false;
    while (!created) {
      try {
        int randomPort = selectRandomPort(1024, 65535);
        int randomInternalPort = selectRandomPort(1024, 65535);
        Map<String, String> masterConf = new HashMap<>();
        masterConf.put(CelebornConf.MASTER_HOST().key(), "localhost");
        masterConf.put(CelebornConf.PORT_MAX_RETRY().key(), "0");
        masterConf.put(CelebornConf.MASTER_PORT().key(), String.valueOf(randomPort));
        masterConf.put(CelebornConf.MASTER_ENDPOINTS().key(), "localhost:" + randomPort);
        masterConf.put(
            CelebornConf.MASTER_INTERNAL_PORT().key(), String.valueOf(randomInternalPort));
        masterConf.put(
            CelebornConf.MASTER_INTERNAL_ENDPOINTS().key(), "localhost:" + randomInternalPort);
        masterConf.put(CelebornConf.MASTER_HOST().key(), "localhost");
        Map<String, String> workerConf = new HashMap<>();
        workerConf.put(CelebornConf.MASTER_ENDPOINTS().key(), "localhost:" + randomPort);
        workerConf.put(
            CelebornConf.MASTER_INTERNAL_ENDPOINTS().key(), "localhost:" + randomInternalPort);
        Logger.info(
            "generated configuration. master conf: {} worker conf {}", masterConf, workerConf);
        setUpMiniCluster(masterConf, workerConf);
        created = true;
      } catch (Exception e) {
        Logger.error("Failed to setup mini cluster", e);
        retryCount += 1;
        if (retryCount == 3) {
          Logger.error("Failed to setup mini cluster, reached the max retry count");
          throw e;
        }
      }
    }
  }

  public static void setUpMiniCluster(
      Map<String, String> masterConf, Map<String, String> workerConf)
      throws InterruptedException, BindException {
    int workerNum = 3;
    int timeout = 30000;
    master = createMaster(masterConf);
    AtomicBoolean masterStarted = new AtomicBoolean(false);
    masterThread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  masterStarted.set(true);
                  master.rpcEnv().awaitTermination();
                } catch (Exception e) {
                  masterStarted.set(false);
                  throw e;
                }
              }
            });

    masterThread.start();
    int masterStartWaitingTime = 0;
    Thread.sleep(5000);
    while (!masterStarted.get()) {
      masterStartWaitingTime += 5000;
      if (masterStartWaitingTime >= timeout) {
        throw new BindException("cannot start master rpc endpoint");
      }
    }

    List<Thread> threads = new ArrayList<>();
    List<Worker> workers = new ArrayList<>();
    ReentrantLock flagUpdateLock = new ReentrantLock();
    for (int i = 0; i < workerNum; i++) {
      int finalI = i;
      Thread workerThread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  int workerStartRetry = 0;
                  boolean workerStarted = false;
                  while (!workerStarted) {
                    try {
                      Worker worker = createWorker(workerConf, createTmpDir());
                      flagUpdateLock.lock();
                      workers.add(worker);
                      flagUpdateLock.unlock();
                      workerStarted = true;
                      worker.initialize();
                    } catch (Exception e) {
                      Logger.error("start worker failed", e);
                      if (workers.get(finalI) != null) {
                        workers.get(finalI).shutdown();
                      }
                      workerStarted = false;
                      workerStartRetry += 1;
                      if (workerStartRetry == 3) {
                        try {
                          throw e;
                        } catch (InterruptedException ex) {
                          throw new RuntimeException(ex);
                        } catch (IOException ex) {
                          throw new RuntimeException(ex);
                        }
                      } else {
                        try {
                          TimeUnit.SECONDS.sleep((long) Math.pow(2, workerStartRetry));
                        } catch (InterruptedException ex) {
                          throw new RuntimeException(ex);
                        }
                      }
                      throw new RuntimeException(e);
                    }
                  }
                }
              });
      workerThread.setName("worker-" + i + " starter thread");
      threads.add(workerThread);
    }

    threads.forEach(Thread::start);
    Thread.sleep(5000);
    boolean allWorkersStarted = false;
    int workersWaitingTime = 0;
    while (!allWorkersStarted) {
      try {
        for (int i = 0; i < workerNum; i++) {
          if (workers.get(i) == null) {
            throw new IllegalStateException("worker " + i + " hasn't been initialized");
          } else {
            workerInfos.put(workers.get(i), threads.get(i));
          }
        }
        workerInfos.forEach((k, v) -> Assert.assertTrue(k.registered().get()));
        allWorkersStarted = true;
      } catch (Exception e) {
        Logger.error("all workers haven't been started retrying", e);
        Thread.sleep(5000);
        workersWaitingTime += 5000;
        if (workersWaitingTime >= timeout) {
          Logger.error("cannot start all workers after {} ms", timeout, e);
        }
      }
    }
  }

  public static Master createMaster(Map<String, String> masterConf) throws InterruptedException {
    CelebornConf celebornConf = new CelebornConf();
    celebornConf.set(CelebornConf.METRICS_ENABLED().key(), "false");
    int httpPort = selectRandomPort(1024, 65525);
    celebornConf.set(CelebornConf.MASTER_HTTP_PORT().key(), String.valueOf(httpPort));
    if (masterConf != null) {
      masterConf.forEach((k, v) -> celebornConf.set(k, v));
    }
    MasterArguments arguments = new MasterArguments(new String[] {}, celebornConf);
    Master master = new Master(celebornConf, arguments);
    master.startHttpServer();

    Thread.sleep(5000L);
    return master;
  }

  public static Worker createWorker(Map<String, String> map, String dir)
      throws InterruptedException {
    Logger.info("start create worker for mini cluster");
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_STORAGE_DIRS().key(), dir);
    conf.set(CelebornConf.WORKER_DISK_MONITOR_ENABLED().key(), "false");
    conf.set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "256K");
    conf.set(CelebornConf.WORKER_HTTP_PORT().key(), String.valueOf(selectRandomPort(1024, 65535)));
    conf.set("celeborn.fetch.io.threads", "4");
    conf.set("celeborn.push.io.threads", "4");
    if (map != null) {
      map.forEach((k, v) -> conf.set(k, v));
    }
    WorkerArguments arguments = new WorkerArguments(new String[] {}, conf);
    Worker worker = new Worker(conf, arguments);
    return worker;
  }

  public static String createTmpDir() throws IOException {
    Path tmpDir = Files.createTempDirectory("celeborn");
    tmpDir.toFile().deleteOnExit();
    return tmpDir.toAbsolutePath().toString();
  }

  public static void shutdownMiniCluster() throws InterruptedException {
    workerInfos.forEach(
        (k, v) -> {
          k.stop(CelebornExitKind.EXIT_IMMEDIATELY());
          k.rpcEnv().shutdown();
        });

    master.stop(CelebornExitKind.EXIT_IMMEDIATELY());
    master.rpcEnv().shutdown();

    Thread.sleep(5000);
    workerInfos.forEach(
        (k, v) -> {
          k.stop(CelebornExitKind.EXIT_IMMEDIATELY());
          v.interrupt();
        });
    workerInfos.clear();
    masterThread.interrupt();
    MemoryManager.reset();
  }
}
