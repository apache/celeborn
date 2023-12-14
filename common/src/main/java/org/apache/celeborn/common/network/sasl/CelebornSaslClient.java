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

import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SASL Client for Celeborn which simply keeps track of the state of a single SASL session, from
 * the initial state to the "authenticated" state. This client initializes the protocol via a
 * firstToken, which is then followed by a set of challenges and responses.
 *
 * <p>TODO: Currently, we have hardcoded Sasl mechanism to be DIGEST-MD5 for connection auth. We
 * should make this configurable in the future (see:
 * https://issues.apache.org/jira/browse/CELEBORN-1158).
 */
public class CelebornSaslClient {
  private static final Logger logger = LoggerFactory.getLogger(CelebornSaslClient.class);

  @GuardedBy("this")
  private SaslClient saslClient;

  public CelebornSaslClient(
      String saslMechanism,
      @Nullable Map<String, String> saslProps,
      @Nullable CallbackHandler authCallbackHandler) {
    Preconditions.checkNotNull(saslMechanism);
    try {
      this.saslClient =
          Sasl.createSaslClient(
              new String[] {saslMechanism},
              null,
              null,
              DEFAULT_REALM,
              saslProps,
              authCallbackHandler);
    } catch (SaslException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Used to initiate SASL handshake with server. */
  public synchronized byte[] firstToken() {
    if (saslClient != null && saslClient.hasInitialResponse()) {
      try {
        return saslClient.evaluateChallenge(EMPTY_BYTE_ARRAY);
      } catch (SaslException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      return EMPTY_BYTE_ARRAY;
    }
  }

  /** Determines whether the authentication exchange has completed. */
  public synchronized boolean isComplete() {
    return saslClient != null && saslClient.isComplete();
  }

  /** Returns the value of a negotiated property. */
  public synchronized Object getNegotiatedProperty(String name) {
    return saslClient.getNegotiatedProperty(name);
  }

  /**
   * Respond to server's SASL token.
   *
   * @param token contains server's SASL token
   * @return client's response SASL token
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslClient != null ? saslClient.evaluateChallenge(token) : EMPTY_BYTE_ARRAY;
    } catch (SaslException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the SaslClient might be
   * using.
   */
  public synchronized void dispose() {
    if (saslClient != null) {
      try {
        saslClient.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslClient = null;
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler that works with share secrets.
   */
  static class ClientCallbackHandler implements CallbackHandler {

    private final String id;
    private final String password;

    public ClientCallbackHandler(String id, String password) {
      this.id = Preconditions.checkNotNull(id, "id");
      this.password = Preconditions.checkNotNull(password, "password");
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL client callback: setting username");
          NameCallback nc = (NameCallback) callback;
          nc.setName(encodeIdentifier(id));
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL client callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(encodePassword(password));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL client callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof RealmChoiceCallback) {
          // ignore (?)
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized callback: " + callback);
        }
      }
    }
  }
}
