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

package org.apache.celeborn.common.network;

import static org.apache.celeborn.common.util.JavaUtils.getLocalHost;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

public class RpcIntegrationSuiteJ {
  static String TEST_MODULE = "shuffle";
  static TransportConf conf;
  static TransportContext context;
  static TransportServer server;
  static TransportClientFactory clientFactory;
  static BaseMessageHandler handler;
  static List<String> oneWayMsgs;
  static StreamTestHelper testData;

  @BeforeClass
  public static void setUp() throws Exception {
    initialize((new CelebornConf()));
  }

  static void initialize(CelebornConf celebornConf) throws Exception {
    conf = new TransportConf(TEST_MODULE, celebornConf);
    testData = new StreamTestHelper();
    handler =
        new BaseMessageHandler() {
          @Override
          public void receive(TransportClient client, RequestMessage message) {
            assertTrue(message instanceof OneWayMessage);
            String msg;
            try {
              msg = JavaUtils.bytesToString(message.body().nioByteBuffer());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            oneWayMsgs.add(msg);
          }

          @Override
          public void receive(
              TransportClient client, RequestMessage requestMessage, RpcResponseCallback callback) {
            try {
              String msg = JavaUtils.bytesToString(requestMessage.body().nioByteBuffer());
              String[] parts = msg.split("/");
              if (parts[0].equals("hello")) {
                callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
              } else if (parts[0].equals("return error")) {
                callback.onFailure(new RuntimeException("Returned: " + parts[1]));
              } else if (parts[0].equals("throw error")) {
                callback.onFailure(new RuntimeException("Thrown: " + parts[1]));
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public boolean checkRegistered() {
            return true;
          }
        };
    context = new TransportContext(conf, handler);
    server = context.createServer();
    clientFactory = context.createClientFactory();
    oneWayMsgs = new ArrayList<>();
  }

  @AfterClass
  public static void tearDown() {
    server.close();
    clientFactory.close();
    context.close();
    testData.cleanup();
  }

  static class RpcResult {
    public Set<String> successMessages;
    public Set<String> errorMessages;
  }

  private RpcResult sendRPC(String... commands) throws Exception {
    TransportClient client = clientFactory.createClient(getLocalHost(), server.getPort());
    final Semaphore sem = new Semaphore(0);

    final RpcResult res = new RpcResult();
    res.successMessages = Collections.synchronizedSet(new HashSet<String>());
    res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer message) {
            String response = JavaUtils.bytesToString(message);
            res.successMessages.add(response);
            sem.release();
          }

          @Override
          public void onFailure(Throwable e) {
            res.errorMessages.add(e.getMessage());
            sem.release();
          }
        };

    for (String command : commands) {
      client.sendRpc(JavaUtils.stringToBytes(command), callback);
    }

    if (!sem.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }
    client.close();
    return res;
  }

  @Test
  public void singleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void doubleRPC() throws Exception {
    RpcResult res = sendRPC("hello/Aaron", "hello/Reynold");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!", "Hello, Reynold!"));
    assertTrue(res.errorMessages.isEmpty());
  }

  @Test
  public void returnErrorRPC() throws Exception {
    RpcResult res = sendRPC("return error/OK");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
  }

  @Test
  public void throwErrorRPC() throws Exception {
    RpcResult res = sendRPC("throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
  }

  @Test
  public void doubleTrouble() throws Exception {
    RpcResult res = sendRPC("return error/OK", "throw error/uh-oh");
    assertTrue(res.successMessages.isEmpty());
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK", "Thrown: uh-oh"));
  }

  @Test
  public void sendSuccessAndFailure() throws Exception {
    RpcResult res = sendRPC("hello/Bob", "throw error/the", "hello/Builder", "return error/!");
    assertEquals(res.successMessages, Sets.newHashSet("Hello, Bob!", "Hello, Builder!"));
    assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: the", "Returned: !"));
  }

  @Test
  public void sendOneWayMessage() throws Exception {
    final String message = "no reply";
    TransportClient client = clientFactory.createClient(getLocalHost(), server.getPort());
    try {
      client.send(JavaUtils.stringToBytes(message));
      assertEquals(0, client.getHandler().numOutstandingRequests());

      // Make sure the message arrives.
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (System.nanoTime() < deadline && oneWayMsgs.size() == 0) {
        TimeUnit.MILLISECONDS.sleep(10);
      }

      assertEquals(1, oneWayMsgs.size());
      assertEquals(message, oneWayMsgs.get(0));
    } finally {
      client.close();
    }
  }

  private void assertErrorsContain(Set<String> errors, Set<String> contains) {
    assertEquals(
        "Expected " + contains.size() + " errors, got " + errors.size() + "errors: " + errors,
        contains.size(),
        errors.size());

    Pair<Set<String>, Set<String>> r = checkErrorsContain(errors, contains);
    assertTrue(
        "Could not find error containing " + r.getRight() + "; errors: " + errors,
        r.getRight().isEmpty());

    assertTrue(r.getLeft().isEmpty());
  }

  private Pair<Set<String>, Set<String>> checkErrorsContain(
      Set<String> errors, Set<String> contains) {
    Set<String> remainingErrors = Sets.newHashSet(errors);
    Set<String> notFound = Sets.newHashSet();
    for (String contain : contains) {
      Iterator<String> it = remainingErrors.iterator();
      boolean foundMatch = false;
      while (it.hasNext()) {
        if (it.next().contains(contain)) {
          it.remove();
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        notFound.add(contain);
      }
    }
    return new ImmutablePair<>(remainingErrors, notFound);
  }
}
