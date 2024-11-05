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

package org.apache.celeborn.cli.master

import picocli.CommandLine.Option

final class RatisOptions {

  @Option(
    names = Array("--peerAddress"),
    description = Array("The peer address in form of host:port."))
  private[master] var peerAddress: String = _

  @Option(
    names = Array("--peers"),
    description = Array("The comma separated list of peers in" +
      " the format id=host:port, e.g. `a=host1:9872,b=host2:9872`."))
  private[master] var peers: String = _

  @Option(
    names = Array("--priorities"),
    description = Array("The comma separated list of peers in" +
      " the format host:port=priority, e.g. `host1:9872=0,host2:9872=1`."))
  private[master] var priorities: String = _

}
