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

package org.apache.celeborn.common.network.sasl.anonymous;

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import com.google.common.base.Preconditions;

/**
 * This implements the {@code SaslClientFactory} for the ANONYMOUS SASL mechanism. It allows the
 * creation of SASL clients that can perform ANONYMOUS authentication with a remote server.
 */
public class AnonymousSaslClientFactory implements SaslClientFactory {

  /**
   * Creates a SASL client for the ANONYMOUS mechanism.
   *
   * @param mechanisms The list of SASL mechanisms.
   * @param authorizationId The authorization ID, typically null for ANONYMOUS.
   * @param protocol The name of the protocol being used.
   * @param serverName The name of the server.
   * @param props A map of properties to configure the SASL client.
   * @param cbh A callback handler for handling challenges.
   * @return A {@code CelebornAnonymousSaslClient} instance if ANONYMOUS is requested, or null
   *     otherwise.
   * @throws SaslException
   */
  @Override
  public SaslClient createSaslClient(
      String[] mechanisms,
      String authorizationId,
      String protocol,
      String serverName,
      Map<String, ?> props,
      CallbackHandler cbh)
      throws SaslException {
    Preconditions.checkNotNull(mechanisms);
    for (String mech : mechanisms) {
      if (mech.equals(ANONYMOUS)) {
        return new CelebornAnonymousSaslClient();
      }
    }
    return null;
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return new String[] {ANONYMOUS};
  }

  static class CelebornAnonymousSaslClient implements SaslClient {

    private boolean isCompleted = false;

    @Override
    public String getMechanismName() {
      return ANONYMOUS;
    }

    @Override
    public boolean hasInitialResponse() {
      return false;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
      if (isCompleted) {
        throw new IllegalStateException("Authentication has already completed.");
      }
      isCompleted = true;
      return ANONYMOUS.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isComplete() {
      return isCompleted;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
      throw new IllegalStateException("ANONYMOUS mechanism does not support wrap/unwrap");
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
      throw new IllegalStateException("ANONYMOUS mechanism does not support wrap/unwrap");
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
      return null;
    }

    @Override
    public void dispose() throws SaslException {
      // No resources to cleanup for ANONYMOUS
    }
  }
}
