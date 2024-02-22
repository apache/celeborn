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

package org.apache.celeborn.common.network.registration.anonymous;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.CelebornException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.registration.RegistrationInfo;
import org.apache.celeborn.common.network.sasl.SaslTimeoutException;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbRegisterApplicationRequest;
import org.apache.celeborn.common.protocol.PbRegisterApplicationResponse;

public class RegistrationClientAnonymousBootstrap implements TransportClientBootstrap {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistrationClientAnonymousBootstrap.class);
  private final TransportConf conf;
  private final String appId;
  private final RegistrationInfo registrationInfo;

  public RegistrationClientAnonymousBootstrap(
      TransportConf conf, String appId, RegistrationInfo registrationInfo) {
    this.conf = Preconditions.checkNotNull(conf, "conf");
    this.appId = Preconditions.checkNotNull(appId, "appId");
    this.registrationInfo = Preconditions.checkNotNull(registrationInfo, "registrationInfo");
  }

  @Override
  public void doBootstrap(TransportClient client) throws RuntimeException {
    if (registrationInfo.getRegistrationState() == RegistrationInfo.RegistrationState.REGISTERED) {
      LOG.info("client has already registered, skip register.");
      return;
    }
    try {
      register(client);
      LOG.info("Registration for {}", appId);
      registrationInfo.setRegistrationState(RegistrationInfo.RegistrationState.REGISTERED);
    } catch (IOException | CelebornException e) {
      throw new RuntimeException(e);
    } finally {
      if (registrationInfo.getRegistrationState()
          != RegistrationInfo.RegistrationState.REGISTERED) {
        registrationInfo.setRegistrationState(RegistrationInfo.RegistrationState.FAILED);
      }
    }
  }

  private void register(TransportClient client) throws IOException, CelebornException {
    TransportMessage msg =
        new TransportMessage(
            MessageType.REGISTER_APPLICATION_REQUEST,
            PbRegisterApplicationRequest.newBuilder()
                .setId(appId)
                .setSecret("anonymous")
                .build()
                .toByteArray());
    ByteBuffer response;
    try {
      response = client.sendRpcSync(msg.toByteBuffer(), conf.saslTimeoutMs());
    } catch (RuntimeException ex) {
      // We know it is a Sasl timeout here if it is a TimeoutException.
      if (ex.getCause() instanceof TimeoutException) {
        throw new SaslTimeoutException(ex.getCause());
      } else {
        throw ex;
      }
    }
    PbRegisterApplicationResponse registerApplicationResponse =
        TransportMessage.fromByteBuffer(response).getParsedPayload();
    if (!registerApplicationResponse.getStatus()) {
      throw new CelebornException("Application registration failed. AppId = " + appId);
    }
  }
}
