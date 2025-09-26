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

package org.apache.celeborn.common.network.sasl.registration;

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.exception.CelebornException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.sasl.CelebornSaslClient;
import org.apache.celeborn.common.network.sasl.SaslClientBootstrap;
import org.apache.celeborn.common.network.sasl.SaslCredentials;
import org.apache.celeborn.common.network.sasl.SaslTimeoutException;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbAuthType;
import org.apache.celeborn.common.protocol.PbAuthenticationInitiationRequest;
import org.apache.celeborn.common.protocol.PbAuthenticationInitiationResponse;
import org.apache.celeborn.common.protocol.PbRegisterApplicationRequest;
import org.apache.celeborn.common.protocol.PbRegisterApplicationResponse;
import org.apache.celeborn.common.protocol.PbSaslMechanism;
import org.apache.celeborn.common.protocol.PbSaslRequest;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * Bootstraps a {@link TransportClient} by registering application (if the application is not
 * registered). If the application is already registered, it will bootstrap the client by performing
 * SASL authentication.
 */
public class RegistrationClientBootstrap implements TransportClientBootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(RegistrationClientBootstrap.class);

  private static final String VERSION = "1.0";

  /**
   * TODO: This should be made configurable. For now, we only support ANONYMOUS for client-auth and
   * DIGEST-MD5 for connect-auth.
   */
  private static final List<PbSaslMechanism> SASL_MECHANISMS =
      Lists.newArrayList(
          PbSaslMechanism.newBuilder()
              .setMechanism(ANONYMOUS)
              .addAuthTypes(PbAuthType.CLIENT_AUTH)
              .build(),
          PbSaslMechanism.newBuilder()
              .setMechanism(DIGEST_MD5)
              .addAuthTypes(PbAuthType.CONNECTION_AUTH)
              .build());

  private final TransportConf conf;
  private final String appId;
  private final SaslCredentials saslCredentials;

  private final RegistrationInfo registrationInfo;

  public RegistrationClientBootstrap(
      TransportConf conf,
      String appId,
      SaslCredentials saslCredentials,
      RegistrationInfo registrationInfo) {
    this.conf = Preconditions.checkNotNull(conf, "conf");
    this.appId = Preconditions.checkNotNull(appId, "appId");
    this.saslCredentials = Preconditions.checkNotNull(saslCredentials, "saslCredentials");
    this.registrationInfo = Preconditions.checkNotNull(registrationInfo, "registrationInfo");
  }

  @Override
  public void doBootstrap(TransportClient client) throws RuntimeException {
    if (registrationInfo.getRegistrationState() == RegistrationInfo.RegistrationState.REGISTERED) {
      LOG.info("client has already registered, skip register.");
      doSaslBootstrap(client);
      return;
    }
    try {
      LOG.info("authentication initiation started for {}", appId);
      doAuthInitiation(client);
      LOG.info("authentication initiation successful for {}", appId);
      doClientAuthentication(client);
      LOG.info("client authenticated for {}", appId);
      register(client);
      LOG.info("Registration for {}", appId);
      registrationInfo.setRegistrationState(RegistrationInfo.RegistrationState.REGISTERED);
      client.setClientId(appId);
    } catch (IOException | CelebornException e) {
      // If RPC failure indicates MasterNotLeaderException was the cause, try to reverse engineer it
      // from the exception thrown
      throw new RuntimeException(processMasterNotLeaderException(e));
    } finally {
      if (registrationInfo.getRegistrationState()
          != RegistrationInfo.RegistrationState.REGISTERED) {
        registrationInfo.setRegistrationState(RegistrationInfo.RegistrationState.FAILED);
      }
    }
  }

  private void doAuthInitiation(TransportClient client) throws IOException, CelebornException {
    PbAuthenticationInitiationRequest authInitRequest =
        PbAuthenticationInitiationRequest.newBuilder()
            .setVersion(VERSION)
            .setAuthEnabled(true)
            .addAllSaslMechanisms(SASL_MECHANISMS)
            .build();
    TransportMessage msg =
        new TransportMessage(
            MessageType.AUTHENTICATION_INITIATION_REQUEST, authInitRequest.toByteArray());
    ByteBuffer authInitResponseBuffer;
    try {
      authInitResponseBuffer = client.sendRpcSync(msg.toByteBuffer(), conf.saslTimeoutMs());
    } catch (RuntimeException ex) {
      if (ex.getCause() instanceof TimeoutException) {
        throw new SaslTimeoutException(ex.getCause());
      } else {
        throw ex;
      }
    }
    PbAuthenticationInitiationResponse authInitResponse =
        TransportMessage.fromByteBuffer(authInitResponseBuffer).getParsedPayload();
    if (!validateServerResponse(authInitResponse)) {
      String exMsg =
          "Registration failed due to incompatibility with the server."
              + " InitRequest: "
              + authInitRequest
              + " InitResponse: "
              + authInitResponse;
      throw new CelebornException(exMsg);
    }
    // TODO: client validates required/supported mechanism is present
  }

  private void doClientAuthentication(TransportClient client) throws IOException {
    // Client will authenticate itself with the selected SaslMechanism for Client Authentication
    CelebornSaslClient saslClient = new CelebornSaslClient(ANONYMOUS, null, null);
    try {
      byte[] payload = saslClient.firstToken();
      while (!saslClient.isComplete()) {
        TransportMessage msg =
            new TransportMessage(
                MessageType.SASL_REQUEST,
                PbSaslRequest.newBuilder()
                    .setMethod(ANONYMOUS)
                    .setAuthType(PbAuthType.CLIENT_AUTH)
                    .setPayload(ByteString.copyFrom(payload))
                    .build()
                    .toByteArray());
        ByteBuffer response;
        try {
          LOG.info("Sending SASL message for client authentication");
          response = client.sendRpcSync(msg.toByteBuffer(), conf.saslTimeoutMs());
        } catch (RuntimeException ex) {
          // We know it is a Sasl timeout here if it is a TimeoutException.
          if (ex.getCause() instanceof TimeoutException) {
            throw new SaslTimeoutException(ex.getCause());
          } else {
            throw ex;
          }
        }
        payload = saslClient.response(JavaUtils.bufferToArray(response));
      }

    } finally {
      try { // Once authentication is complete, the server will trust all remaining communication.
        saslClient.dispose();
      } catch (RuntimeException e) {
        LOG.warn("Error while disposing SASL client", e);
      }
    }
  }

  private void register(TransportClient client) throws IOException, CelebornException {
    TransportMessage msg =
        new TransportMessage(
            MessageType.REGISTER_APPLICATION_REQUEST,
            PbRegisterApplicationRequest.newBuilder()
                .setId(appId)
                .setSecret(saslCredentials.getPassword())
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

  private void doSaslBootstrap(TransportClient client) {
    SaslClientBootstrap bootstrap = new SaslClientBootstrap(conf, appId, saslCredentials);
    bootstrap.doBootstrap(client);
  }

  private boolean validateServerResponse(PbAuthenticationInitiationResponse authInitResponse) {
    if (!authInitResponse.getVersion().equals(VERSION)) {
      return false;
    }
    Map<PbAuthType, Set<String>> serverSupportedMechs =
        findSupportedSaslMechs(authInitResponse.getSaslMechanismsList());
    Set<String> clientAuthMechs = serverSupportedMechs.get(PbAuthType.CLIENT_AUTH);
    if (clientAuthMechs == null) {
      return false;
    }
    if (!clientAuthMechs.contains(ANONYMOUS)) {
      return false;
    }
    Set<String> connectionAuthMechs = serverSupportedMechs.get(PbAuthType.CONNECTION_AUTH);
    if (connectionAuthMechs == null) {
      return false;
    }
    return connectionAuthMechs.contains(DIGEST_MD5);
  }

  private static Map<PbAuthType, Set<String>> findSupportedSaslMechs(
      List<PbSaslMechanism> serverSupportedMechs) {
    Map<PbAuthType, Set<String>> supportedMechs = new HashMap<>();
    for (PbSaslMechanism mech : serverSupportedMechs) {
      for (PbAuthType authType : mech.getAuthTypesList()) {
        Set<String> mechanisms = supportedMechs.computeIfAbsent(authType, k -> Sets.newHashSet());
        mechanisms.add(mech.getMechanism());
      }
    }
    return supportedMechs;
  }

  @SuppressWarnings("NonInclusiveLanguage")
  private static final Pattern MASTER_NOT_LEADER_EXCEPTION_PATTERN =
      Pattern.compile(
          "^.*MasterNotLeaderException: Master:([^ ]*) is not the leader. Suggested leader is "
              + "Master:\\(([^,]*),[^)]*\\) \\(\\(([^,]*),[^)]*\\)\\).*$",
          Pattern.MULTILINE | Pattern.DOTALL);

  @SuppressWarnings("NonInclusiveLanguage")
  public static Exception processMasterNotLeaderException(Exception ex) {
    String stringified = Throwables.getStackTraceAsString(ex);

    Matcher matcher = MASTER_NOT_LEADER_EXCEPTION_PATTERN.matcher(stringified);
    if (matcher.matches()) {
      // MasterNotLeaderException was raised, pull the 'parts' and reconstruct the exception
      String currentPeer = matcher.group(1);
      String suggestedLeaderPeer = matcher.group(2);
      String suggestedInternalLeaderPeer = matcher.group(3);

      return new MasterNotLeaderException(
          currentPeer,
          Tuple2.apply(suggestedLeaderPeer, suggestedLeaderPeer),
          Tuple2.apply(suggestedInternalLeaderPeer, suggestedInternalLeaderPeer),
          // does not matter - both values are same
          true,
          null);
    } else {
      return ex;
    }
  }
}
