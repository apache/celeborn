package org.apache.celeborn.common.network.registration.anonymous;

import io.netty.channel.Channel;

import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;

public class RegistrationServerAnonymousBootstrap implements TransportServerBootstrap {
  private final TransportConf conf;
  private final SecretRegistry secretRegistry;

  public RegistrationServerAnonymousBootstrap(TransportConf conf, SecretRegistry secretRegistry) {
    this.conf = conf;
    this.secretRegistry = secretRegistry;
  }

  @Override
  public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler rpcHandler) {
    return new RegistrationAnonymousRpcHandler(conf, channel, rpcHandler, secretRegistry);
  }
}
