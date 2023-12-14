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

package org.apache.celeborn.common.network.sasl;

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbAuthType;
import org.apache.celeborn.common.protocol.PbSaslRequest;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * Bootstraps a {@link TransportClient} by performing SASL authentication on the connection. The
 * server should be setup with a {@code SaslRpcHandler} with matching keys for the given appId.
 */
public class SaslClientBootstrap implements TransportClientBootstrap {
  private static final Logger logger = LoggerFactory.getLogger(SaslClientBootstrap.class);

  private final TransportConf conf;
  private final String appId;
  private final SaslCredentials saslCredentials;

  /**
   * Creates a new SaslClientBootstrap. When this is used for authenticating a client connection,
   * the user id in the saslCredentials should be the same as the appId.
   *
   * @param conf transport conf
   * @param appId application id
   * @param saslCredentials sasl userId and password.
   */
  public SaslClientBootstrap(TransportConf conf, String appId, SaslCredentials saslCredentials) {
    this.conf = conf;
    this.appId = appId;
    this.saslCredentials = Preconditions.checkNotNull(saslCredentials);
  }

  /**
   * Performs SASL authentication by sending a token, and then proceeding with the SASL
   * challenge-response tokens until we either successfully authenticate or throw an exception due
   * to mismatch.
   */
  @Override
  public void doBootstrap(TransportClient client, Channel channel) {
    // TODO: Hardcoding the SASL mechanism to DIGEST-MD5 for Connection Authentication. This
    // should be configurable in the future.
    CelebornSaslClient saslClient =
        new CelebornSaslClient(
            DIGEST_MD5,
            DEFAULT_SASL_CLIENT_PROPS,
            new CelebornSaslClient.ClientCallbackHandler(
                saslCredentials.getUserId(), saslCredentials.getPassword()));
    try {
      byte[] payload = saslClient.firstToken();
      boolean firstToken = true;
      while (!saslClient.isComplete()) {
        PbSaslRequest.Builder builder = PbSaslRequest.newBuilder();
        if (firstToken) {
          builder.setMethod(DIGEST_MD5).setAuthType(PbAuthType.CONNECTION_AUTH);
        }
        TransportMessage msg =
            new TransportMessage(
                MessageType.SASL_REQUEST,
                builder.setPayload(ByteString.copyFrom(payload)).build().toByteArray());
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
        // The response of a SaslMessage is either a RpcResponse or a RpcFailure.
        payload = saslClient.response(JavaUtils.bufferToArray(response));
        firstToken = false;
      }
      client.setClientId(appId);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      try {
        // Once authentication is complete, the server will trust all remaining communication.
        saslClient.dispose();
      } catch (RuntimeException e) {
        logger.error("Error while disposing SASL client", e);
      }
    }
  }
}
