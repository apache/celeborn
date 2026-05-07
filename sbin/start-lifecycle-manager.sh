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

# Starts a standalone Celeborn LifecycleManager daemon in background.
# The RPC port is randomly selected from 30000~50000 if --port is not specified.
# After successful start, the port is exported as CELEBORN_LM_PORT.

set -euo pipefail

if [ -z "${CELEBORN_HOME:-}" ]; then
  export CELEBORN_HOME="$(cd "$(dirname "$0")/.."; pwd)"
fi

usage() {
  cat <<EOF
Usage: $0 --app-id <id> --master-endpoints <ep1,ep2,...> [--port <port>] [--host <host>] [--properties-file <file>]

  --app-id              REQUIRED  unique application id
  --master-endpoints    REQUIRED  comma-separated host:port of Celeborn Masters
  --port                OPTIONAL  fixed RPC port to bind (default: random available port in 30000~50000)
  --host                OPTIONAL  bind host (default: hostname)
  --properties-file     OPTIONAL  path to celeborn-defaults.conf
EOF
  exit 1
}

# Find a random available port in range [30000, 50000]
find_available_port() {
  local port
  local max_attempts=100
  local attempt=0
  while [ "$attempt" -lt "$max_attempts" ]; do
    port=$(( RANDOM % 20001 + 30000 ))
    # Check if the port is available (not in use)
    if ! (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
      echo "$port"
      return 0
    fi
    attempt=$(( attempt + 1 ))
  done
  echo "Error: failed to find an available port in 30000~50000 after $max_attempts attempts." >&2
  return 1
}

# Pre-check: required args must be present
have_app_id=0; have_master=0; have_port=0
for ((i=1; i<=$#; i++)); do
  case "${!i}" in
    --app-id) have_app_id=1 ;;
    --master-endpoints) have_master=1 ;;
    --port) have_port=1 ;;
  esac
done

if [ "$have_app_id" -eq 0 ] || [ "$have_master" -eq 0 ]; then
  echo "Error: --app-id and --master-endpoints are required." >&2
  usage
fi

# If --port is not specified, find a random available port and append it to args
if [ "$have_port" -eq 0 ]; then
  CELEBORN_LM_PORT=$(find_available_port)
  echo "Auto-selected available port: ${CELEBORN_LM_PORT}"
  set -- "$@" --port "${CELEBORN_LM_PORT}"
else
  # Extract the port value from args
  for ((i=1; i<=$#; i++)); do
    if [ "${!i}" = "--port" ]; then
      j=$(( i + 1 ))
      CELEBORN_LM_PORT="${!j}"
      break
    fi
  done
fi

export CELEBORN_LM_PORT

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

# --- Background execution ---
LOG_DIR="${CELEBORN_HOME}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/lifecyclemanager-${CELEBORN_LM_PORT}.out"
PID_FILE="${LOG_DIR}/lifecyclemanager-${CELEBORN_LM_PORT}.pid"

nohup "${JAVA}" -cp "${CLASSPATH}" ${CELEBORN_JAVA_OPTS} "${MAIN_CLASS}" "$@" \
  > "${LOG_FILE}" 2>&1 &

CELEBORN_LM_PID=$!
echo "${CELEBORN_LM_PID}" > "${PID_FILE}"

# Wait briefly and verify the process is still alive
sleep 2
if kill -0 "${CELEBORN_LM_PID}" 2>/dev/null; then
  export CELEBORN_LM_PORT
  export CELEBORN_LM_PID

  # Write port to a env file so other scripts can source it
  ENV_FILE="${LOG_DIR}/lifecyclemanager-${CELEBORN_LM_PID}.env"
  cat > "${ENV_FILE}" <<ENVEOF
export CELEBORN_LM_PORT=${CELEBORN_LM_PORT}
export CELEBORN_LM_PID=${CELEBORN_LM_PID}
ENVEOF

  echo "============================================="
  echo " LifecycleManager started successfully!"
  echo "   PID  : ${CELEBORN_LM_PID}"
  echo "   PORT : ${CELEBORN_LM_PORT}"
  echo "   LOG  : ${LOG_FILE}"
  echo "   ENV  : ${ENV_FILE}"
  echo ""
  echo " To load the port in another shell:"
  echo "   source ${ENV_FILE}"
  echo "============================================="
else
  echo "Error: LifecycleManager failed to start. Check log: ${LOG_FILE}" >&2
  cat "${LOG_FILE}" >&2
  rm -f "${PID_FILE}"
  exit 1
fi
