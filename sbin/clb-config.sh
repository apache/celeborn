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

unset HADOOP_CONF_DIR

# included in all the clb scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# symlink and absolute path should rely on CLB_HOME to resolve
if [ -z "${CLB_HOME}" ]; then
  export CLB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export CLB_CONF_DIR="${CLB_CONF_DIR:-"${CLB_HOME}/conf"}"

if [ -z "$CLB_ENV_LOADED" ]; then
  export CLB_ENV_LOADED=1

  if [ -f "${CLB_CONF_DIR}/clb-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${CLB_CONF_DIR}/clb-env.sh"
    set +a
  fi
fi
