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

import org.apache.ratis.shell.cli.sh.RatisShell;
import org.junit.Test;

public class CelebornRatisShellSuitJ {
  @Test
  public void testLoadSefDefinedUtil() {
    RatisShell shell = new RatisShell(System.out);
    shell.run(
        "election",
        "transfer",
        "-peers",
        "ip-10-169-48-202.idata-server.shopee.io:9872,ip-10-169-48-203.idata-server.shopee.io:9872,ip-10-169-48-204.idata-server.shopee.io:9872",
        "-groupid",
        "3ab29dc7-61dd-3e52-9a8d-f9c189140641",
        "-address", "ip-10-169-48-204.idata-server.shopee.io:9872");
  }
}
