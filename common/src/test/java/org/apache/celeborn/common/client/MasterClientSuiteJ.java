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

package org.apache.celeborn.common.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import scala.concurrent.Future;
import scala.concurrent.Future$;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornException;
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplication;
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromWorker;
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromWorkerResponse;
import org.apache.celeborn.common.protocol.message.ControlMessages.OneWayMessageResponse$;
import org.apache.celeborn.common.rpc.RpcAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.rpc.RpcTimeoutException;

public class MasterClientSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(MasterClientSuiteJ.class);


  private final String masterHost = "localhost";
  private final int masterPort = 9097;
  private final CelebornConf conf = new CelebornConf(false);
  private final OneWayMessageResponse$ response = OneWayMessageResponse$.MODULE$;
  private final HeartbeatFromWorkerResponse mockResponse =
      Mockito.mock(HeartbeatFromWorkerResponse.class);

  private RpcEnv rpcEnv = null;
  private RpcEndpointRef endpointRef = null;

  @Before
  public void beforeEach() {
    conf.set(CelebornConf.RPC_ASK_TIMEOUT().key(), "5s");
    conf.set(CelebornConf.NETWORK_TIMEOUT().key(), "5s");
    rpcEnv = Mockito.mock(RpcEnv.class);
    endpointRef = Mockito.mock(RpcEndpointRef.class);
  }

  @Test
  public void testSendOneWayMessageWithoutHA() throws Exception {
    final SettableFuture<Boolean> success = SettableFuture.create();
    final CelebornConf conf = prepareForCelebornConfWithoutHA();

    prepareForEndpointRefWithoutRetry(
        () -> {
          success.set(true);
          return Future$.MODULE$.successful(response);
        });
    prepareForRpcEnvWithoutHA();

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromApplication message = Mockito.mock(HeartbeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSendOneWayMessageWithoutHAWithRetry() throws Exception {
    final AtomicInteger numTries = new AtomicInteger(0);
    final SettableFuture<Boolean> success = SettableFuture.create();
    final CelebornConf conf = prepareForCelebornConfWithoutHA();

    prepareForEndpointRefWithRetry(
        numTries,
        () -> {
          success.set(true);
          return Future$.MODULE$.successful(response);
        });
    prepareForRpcEnvWithoutHA();

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromApplication message = Mockito.mock(HeartbeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
    assertEquals(3, numTries.get());
  }

  @Test
  public void testSendOneWayMessageWithHA() throws Exception {
    final CelebornConf conf = prepareForCelebornConfWithHA();

    final SettableFuture<Boolean> success = SettableFuture.create();

    prepareForRpcEnvWithHA(
        () -> {
          success.set(true);
          return Future$.MODULE$.successful(response);
        });

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromApplication message = Mockito.mock(HeartbeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSendMessageWithoutHA() {
    final CelebornConf conf = prepareForCelebornConfWithoutHA();

    prepareForEndpointRefWithoutRetry(() -> Future$.MODULE$.successful(mockResponse));
    prepareForRpcEnvWithoutHA();

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatFromWorkerResponse response = null;
    try {
      response = client.askSync(message, HeartbeatFromWorkerResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testSendMessageWithoutHAWithRetry() {
    final AtomicInteger numTries = new AtomicInteger(0);
    final CelebornConf conf = prepareForCelebornConfWithoutHA();

    prepareForEndpointRefWithRetry(numTries, () -> Future$.MODULE$.successful(mockResponse));
    prepareForRpcEnvWithoutHA();

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatFromWorkerResponse response = null;
    try {
      response = client.askSync(message, HeartbeatFromWorkerResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testSendMessageWithoutHAWithoutRetry() {
    final AtomicInteger numTries = new AtomicInteger(0);
    final CelebornConf conf =
        prepareForCelebornConfWithoutHA().set(CelebornConf.MASTER_CLIENT_MAX_RETRIES(), 0);

    prepareForEndpointRefWithRetry(numTries, () -> Future$.MODULE$.failed(new IOException()));
    prepareForRpcEnvWithoutHA();

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    Throwable throwable =
        assertThrows(
            Throwable.class, () -> client.askSync(message, HeartbeatFromWorkerResponse.class));
    assertTrue(throwable.getCause() instanceof IOException);
    assertEquals(1, client.getMaxRetries());
  }

  @Test
  public void testSendMessageWithHA() {
    final CelebornConf conf = prepareForCelebornConfWithHA();

    prepareForRpcEnvWithHA(() -> Future$.MODULE$.successful(mockResponse));

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatFromWorkerResponse response = null;
    try {
      response = client.askSync(message, HeartbeatFromWorkerResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testOneMasterDownCausedByIOExceptionInHA() {
    checkOneMasterDownInHA(new IOException("test"));
  }

  @Test
  public void testOneMasterDownCausedByRuntimeExceptionInHA() {
    checkOneMasterDownInHA(new RuntimeException("test"));
  }

  @Test
  public void testOneMasterTimeoutInHA() {
    checkOneMasterAskFailedInHA(new RpcTimeoutException("test", new TimeoutException("test")));
  }

  private void checkOneMasterDownInHA(Exception causedByException) {
    final CelebornConf conf = prepareForCelebornConfWithHA();

    final RpcEndpointRef master1 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef master3 = Mockito.mock(RpcEndpointRef.class);

    // master leader switch to host2
    Mockito.doReturn(
            Future$.MODULE$.failed(new MasterNotLeaderException("host1:9097", "host2:9097", null)))
        .when(master1)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());

    Mockito.doReturn(Future$.MODULE$.successful(mockResponse))
        .when(master3)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());

    Mockito.doAnswer(
            (invocation) -> {
              RpcAddress address = invocation.getArgument(0, RpcAddress.class);
              switch (address.host()) {
                case "host1":
                  return master1;
                case "host2":
                  throw new CelebornException("test", causedByException);
                case "host3":
                  return master3;
                default:
                  fail(
                      "Should use master host1/host2/host3:" + masterPort + ", but use " + address);
              }
              return null;
            })
        .when(rpcEnv)
        .setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatFromWorkerResponse response = null;
    try {
      response = client.askSync(message, HeartbeatFromWorkerResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  private void checkOneMasterAskFailedInHA(Exception exception) {
    final CelebornConf conf = prepareForCelebornConfWithHA();

    final RpcEndpointRef master1 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef master2 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef master3 = Mockito.mock(RpcEndpointRef.class);

    // master leader switch to host2
    Mockito.doReturn(
            Future$.MODULE$.failed(new MasterNotLeaderException("host1:9097", "host2:9097", null)))
        .when(master1)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());

    // Assume master2 get exception.
    Mockito.doReturn(Future$.MODULE$.failed(exception))
        .when(master2)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());

    Mockito.doReturn(Future$.MODULE$.successful(mockResponse))
        .when(master3)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());

    Mockito.doAnswer(
            (invocation) -> {
              RpcAddress address = invocation.getArgument(0, RpcAddress.class);
              switch (address.host()) {
                case "host1":
                  return master1;
                case "host2":
                  return master2;
                case "host3":
                  return master3;
                default:
                  fail(
                      "Should use master host1/host2/host3:" + masterPort + ", but use " + address);
              }
              return null;
            })
        .when(rpcEnv)
        .setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());

    MasterClient client = new MasterClient(rpcEnv, conf, false);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatFromWorkerResponse response = null;
    try {
      response = client.askSync(message, HeartbeatFromWorkerResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  private void prepareForRpcEnvWithHA(final Supplier<Future<?>> supplier) {
    final RpcEndpointRef ref1 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef ref2 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef ref3 = Mockito.mock(RpcEndpointRef.class);

    // master leader switch to host2
    Mockito.doReturn(
            Future$.MODULE$.failed(new MasterNotLeaderException("host1:9097", "host2:9097", null)))
        .when(ref1)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());
    // Assume host2 down.
    Mockito.doReturn(Future$.MODULE$.failed(new IOException("Test IOException")))
        .when(ref2)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());
    // master leader switch to host3 after host2 down.
    Mockito.doReturn(supplier.get()).when(ref3).ask(Mockito.any(), Mockito.any(), Mockito.any());

    Mockito.doAnswer(
            invocation -> {
              RpcAddress address = invocation.getArgument(0, RpcAddress.class);
              if (address.port() == masterPort) {
                switch (address.host()) {
                  case "host1":
                    return ref1;
                  case "host2":
                    return ref2;
                  case "host3":
                    return ref3;
                  default:
                    fail(
                        "Should use master host1/host2/host3:"
                            + masterPort
                            + ", but use "
                            + address);
                }
              } else {
                fail("Should use master host1/host2/host3:" + masterPort + ", but use " + address);
              }
              return null;
            })
        .when(rpcEnv)
        .setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());
  }

  private void prepareForRpcEnvWithoutHA() {
    Mockito.doAnswer(
            (invocationOnMock) -> {
              RpcAddress address = invocationOnMock.getArgument(0, RpcAddress.class);
              if (address.host().equals(masterHost) && address.port() == masterPort) {
                return endpointRef;
              } else {
                fail(
                    "Should only use master + "
                        + masterHost
                        + ":"
                        + masterPort
                        + ", but use "
                        + address);
                return null;
              }
            })
        .when(rpcEnv)
        .setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());
  }

  private void prepareForEndpointRefWithRetry(
      final AtomicInteger numTries, Supplier<Future<?>> supplier) {
    Mockito.doAnswer(
            invocation -> {
              switch (numTries.getAndIncrement()) {
                case 0:
                  return Future$.MODULE$.failed(new IOException("Test 1"));
                case 1:
                  return Future$.MODULE$.failed(new IOException("Test 2"));
                case 2:
                  return supplier.get();
                default:
                  return Future$.MODULE$.failed(new IllegalStateException("too many tries."));
              }
            })
        .when(endpointRef)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());
  }

  private void prepareForEndpointRefWithoutRetry(Supplier<Future<?>> supplier) {
    Mockito.doAnswer(invocation -> supplier.get())
        .when(endpointRef)
        .ask(Mockito.any(), Mockito.any(), Mockito.any());
  }

  private CelebornConf prepareForCelebornConfWithoutHA() {
    return conf.clone()
        .set(CelebornConf.HA_ENABLED().key(), "false")
        .set(CelebornConf.MASTER_ENDPOINTS().key(), masterHost + ":" + masterPort);
  }

  private CelebornConf prepareForCelebornConfWithHA() {
    return conf.clone()
        .set(CelebornConf.HA_ENABLED().key(), "true")
        .set(CelebornConf.MASTER_ENDPOINTS().key(), "host1:9097,host2:9097,host3:9097")
        .set(CelebornConf.MASTER_CLIENT_MAX_RETRIES().key(), "5");
  }
}
