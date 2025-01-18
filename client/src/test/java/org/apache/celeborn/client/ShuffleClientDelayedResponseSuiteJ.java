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

package org.apache.celeborn.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse$;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcEndpointRef;

public class ShuffleClientDelayedResponseSuiteJ {

  private ShuffleClientImpl shuffleClient;
  private final RpcEndpointRef endpointRef = mock(RpcEndpointRef.class);

  private static final String TEST_APPLICATION_ID = "testapp1";

  @Test
  public void testUpdateReducerFileGroupTimeout() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.rpc.getReducerFileGroup.askTimeout", "1ms");
    LifecycleManager lifecycleManager = new LifecycleManager("APP", conf);
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();

    // mock framework will return unexpected result when the answer is delayed
    // and there is multiple call of when
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            new AnswersWithDelay(
                1000,
                invocation ->
                    GetReducerFileGroupResponse$.MODULE$.apply(
                        StatusCode.SUCCESS, locations, new int[0], Collections.emptySet())));

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self());

    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    try {
      shuffleClient.updateFileGroup(0, 0);
    } catch (CelebornIOException e) {
      exceptionRef.set(e);
    }

    Thread.sleep(3000);

    Exception exception = exceptionRef.get();
    Assert.assertTrue(exception.getCause() instanceof TimeoutException);
  }
}
