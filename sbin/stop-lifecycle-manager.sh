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

# Stops a Celeborn LifecycleManager daemon by PID file or port.
# Usage:
#   stop-lifecycle-manager.sh --port <port>    Stop the instance bound to <port>
#   stop-lifecycle-manager.sh --all            Stop all LifecycleManager instances

set -euo pipefail

if [ -z "${CELEBORN_HOME:-}" ]; then
  export CELEBORN_HOME="$(cd "$(dirname "$0")/.."; pwd)"
fi

LOG_DIR="${CELEBORN_HOME}/logs"
GRACEFUL_TIMEOUT=10

usage() {
  cat <<EOF
Usage: $0 --port <port> | --all

  --port <port>   Stop the LifecycleManager instance running on the specified port
  --all           Stop all LifecycleManager instances managed by this script
EOF
  exit 1
}

# Stop a single instance by its PID file
stop_by_pid_file() {
  local pid_file="$1"
  local pid

  if [ ! -f "${pid_file}" ]; then
    echo "PID file not found: ${pid_file}" >&2
    return 1
  fi

  pid=$(cat "${pid_file}")
  if [ -z "${pid}" ]; then
    echo "PID file is empty: ${pid_file}, removing." >&2
    rm -f "${pid_file}"
    return 1
  fi

  if kill -0 "${pid}" 2>/dev/null; then
    echo "Stopping LifecycleManager (PID: ${pid}) with SIGTERM ..."
    kill -TERM "${pid}"

    # Wait for graceful shutdown
    local waited=0
    while [ "${waited}" -lt "${GRACEFUL_TIMEOUT}" ]; do
      if ! kill -0 "${pid}" 2>/dev/null; then
        echo "LifecycleManager (PID: ${pid}) stopped."
        rm -f "${pid_file}"
        # Clean up the env file
        rm -f "${LOG_DIR}/lifecyclemanager-${pid}.env"
        return 0
      fi
      sleep 1
      waited=$(( waited + 1 ))
    done

    # Force kill if still alive
    echo "LifecycleManager (PID: ${pid}) did not stop after ${GRACEFUL_TIMEOUT}s, sending SIGKILL ..."
    kill -9 "${pid}" 2>/dev/null || true
    sleep 1
    rm -f "${pid_file}"
    rm -f "${LOG_DIR}/lifecyclemanager-${pid}.env"
    echo "LifecycleManager (PID: ${pid}) killed."
  else
    echo "LifecycleManager (PID: ${pid}) is not running. Cleaning up stale PID file."
    rm -f "${pid_file}"
    rm -f "${LOG_DIR}/lifecyclemanager-${pid}.env"
  fi
}

# Parse arguments
if [ $# -eq 0 ]; then
  usage
fi

MODE=""
TARGET_PORT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --port)
      MODE="port"
      TARGET_PORT="${2:-}"
      if [ -z "${TARGET_PORT}" ]; then
        echo "Error: --port requires a port number." >&2
        usage
      fi
      shift 2
      ;;
    --all)
      MODE="all"
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

if [ -z "${MODE}" ]; then
  usage
fi

case "${MODE}" in
  port)
    PID_FILE="${LOG_DIR}/lifecyclemanager-${TARGET_PORT}.pid"
    stop_by_pid_file "${PID_FILE}"
    ;;
  all)
    found=0
    for pid_file in "${LOG_DIR}"/lifecyclemanager-*.pid; do
      [ -f "${pid_file}" ] || continue
      found=1
      stop_by_pid_file "${pid_file}"
    done
    if [ "${found}" -eq 0 ]; then
      echo "No running LifecycleManager instances found."
    fi
    ;;
esac
