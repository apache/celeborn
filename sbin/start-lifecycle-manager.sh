#!/usr/bin/env bash
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

# Starts a standalone Celeborn LifecycleManager daemon (one process per Application).
# Runs in foreground; use Ctrl-C or kill -TERM to stop gracefully.

set -euo pipefail

if [ -z "${CELEBORN_HOME:-}" ]; then
  export CELEBORN_HOME="$(cd "$(dirname "$0")/.."; pwd)"
fi

usage() {
  cat <<EOF
Usage: $0 --app-id <id> --master-endpoints <ep1,ep2,...> --port <port> [--host <host>] [--properties-file <file>]

  --app-id              REQUIRED  unique application id
  --master-endpoints    REQUIRED  comma-separated host:port of Celeborn Masters
  --port                REQUIRED  fixed RPC port to bind (must be >=1024)
  --host                OPTIONAL  bind host (default: hostname)
  --properties-file     OPTIONAL  path to celeborn-defaults.conf
EOF
  exit 1
}

# Pre-check: all three required args must be present
have_app_id=0; have_master=0; have_port=0
for ((i=1; i<=$#; i++)); do
  case "${!i}" in
    --app-id) have_app_id=1 ;;
    --master-endpoints) have_master=1 ;;
    --port) have_port=1 ;;
  esac
done

if [ "$have_app_id" -eq 0 ] || [ "$have_master" -eq 0 ] || [ "$have_port" -eq 0 ]; then
  echo "Error: --app-id, --master-endpoints, and --port are required." >&2
  usage
fi

# Load environment (JAVA_HOME, CELEBORN_CONF_DIR, etc.)
# Temporarily disable nounset because load-celeborn-env.sh checks unset vars
if [ -f "${CELEBORN_HOME}/sbin/load-celeborn-env.sh" ]; then
  set +u
  # shellcheck source=/dev/null
  . "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"
  set -u
fi

CELEBORN_CONF_DIR="${CELEBORN_CONF_DIR:-${CELEBORN_HOME}/conf}"

# Determine Java
if [ -n "${JAVA_HOME:-}" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  JAVA="java"
fi

# Build classpath: conf + service jars + client jars + common jars
CLASSPATH="${CELEBORN_CONF_DIR}"
for dir in \
  "${CELEBORN_HOME}/service/target/"*.jar \
  "${CELEBORN_HOME}/client/target/"*.jar \
  "${CELEBORN_HOME}/common/target/"*.jar \
  "${CELEBORN_HOME}/spi/target/"*.jar \
  "${CELEBORN_HOME}/jars/"*.jar \
  "${CELEBORN_HOME}/master-jars/"*.jar \
  "${CELEBORN_HOME}/service-jars/"*.jar \
  "${CELEBORN_HOME}/client-jars/"*.jar; do
  if [ -e "$dir" ]; then
    CLASSPATH="${CLASSPATH}:${dir}"
  fi
done

# JVM options
CELEBORN_JAVA_OPTS="${CELEBORN_JAVA_OPTS:-}"
if [ -f "${CELEBORN_CONF_DIR}/log4j2.xml" ]; then
  CELEBORN_JAVA_OPTS="${CELEBORN_JAVA_OPTS} -Dlog4j2.configurationFile=file:${CELEBORN_CONF_DIR}/log4j2.xml"
fi

MAIN_CLASS="org.apache.celeborn.service.deploy.lifecyclemanager.LifecycleManagerDaemon"

exec "${JAVA}" -cp "${CLASSPATH}" ${CELEBORN_JAVA_OPTS} "${MAIN_CLASS}" "$@"
