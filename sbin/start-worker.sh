#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the clb worker on the machine this script is executed on.

if [ -z "${CLB_HOME}" ]; then
  export CLB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CLB_HOME}/sbin/clb-config.sh"

if [ "$CLB_WORKER_MEMORY" = "" ]; then
  CLB_WORKER_MEMORY="1g"
fi

if [ "$CLB_WORKER_OFFHEAP_MEMORY" = "" ]; then
  CLB_WORKER_OFFHEAP_MEMORY="1g"
fi

export CLB_JAVA_OPTS="-Xmx$CLB_WORKER_MEMORY -XX:MaxDirectMemorySize=$CLB_WORKER_OFFHEAP_MEMORY $CLB_WORKER_JAVA_OPTS"

if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${CLB_HOME}/sbin/clb-daemon.sh" start org.apache.celeborn.service.deploy.worker.Worker "$WORKER_INSTANCE" "$@"