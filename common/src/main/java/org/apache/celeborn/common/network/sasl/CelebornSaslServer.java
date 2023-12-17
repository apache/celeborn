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
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SASL Server for Celeborn which simply keeps track of the state of a single SASL session, from
 * the initial state to the "authenticated" state. (It is not a server in the sense of accepting
 * connections on some socket.)
 */
public class CelebornSaslServer {
  private static final Logger logger = LoggerFactory.getLogger(CelebornSaslServer.class);

  private SaslServer saslServer;

  public CelebornSaslServer(
      String saslMechanism,
      @Nullable Map<String, String> saslProps,
      @Nullable CallbackHandler callbackHandler) {
    Preconditions.checkNotNull(saslMechanism);
    try {
      this.saslServer =
          Sasl.createSaslServer(saslMechanism, null, DEFAULT_REALM, saslProps, callbackHandler);
    } catch (SaslException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Determines whether the authentication exchange has completed successfully. */
  public synchronized boolean isComplete() {
    return saslServer != null && saslServer.isComplete();
  }

  /** Returns the value of a negotiated property. */
  public synchronized Object getNegotiatedProperty(String name) {
    return saslServer.getNegotiatedProperty(name);
  }

  /**
   * Used to respond to server SASL tokens.
   *
   * @param token Server's SASL token
   * @return response to send back to the server.
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslServer != null ? saslServer.evaluateResponse(token) : EMPTY_BYTE_ARRAY;
    } catch (SaslException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the SaslServer might be
   * using.
   */
  public synchronized void dispose() {
    if (saslServer != null) {
      try {
        saslServer.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslServer = null;
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler for SASL DIGEST-MD5 mechanism.
   */
  static class DigestCallbackHandler implements CallbackHandler {
    private final SecretRegistry secretKeyHolder;

    /**
     * The use of 'volatile' is not necessary here because the 'handle' invocation includes both the
     * NameCallback and PasswordCallback (with the name preceding the password), all within the same
     * thread.
     */
    private String userName = null;

    DigestCallbackHandler(SecretRegistry secretRegistry) {
      this.secretKeyHolder = Preconditions.checkNotNull(secretRegistry);
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException, SaslException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL server callback: setting username");
          NameCallback nc = (NameCallback) callback;
          String encodedName = nc.getName() != null ? nc.getName() : nc.getDefaultName();
          if (encodedName == null) {
            throw new SaslException("No username provided by client");
          }
          userName = decodeIdentifier(encodedName);
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL server callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          String secret = secretKeyHolder.getSecretKey(userName);
          if (secret == null) {
            // TODO: CELEBORN-1179 Add support for fetching the secret from the Celeborn master.
            throw new RuntimeException("Registration information not found for " + userName);
          }
          pc.setPassword(encodePassword(secret));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL server callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof AuthorizeCallback) {
          AuthorizeCallback ac = (AuthorizeCallback) callback;
          String authId = ac.getAuthenticationID();
          String authzId = ac.getAuthorizationID();
          ac.setAuthorized(authId.equals(authzId));
          if (ac.isAuthorized()) {
            ac.setAuthorizedID(authzId);
          }
          logger.debug("SASL Authorization complete, authorized set to {}", ac.isAuthorized());
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized callback: " + callback);
        }
      }
    }
  }
}
