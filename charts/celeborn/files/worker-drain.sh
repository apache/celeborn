#!/bin/sh
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
# preStop hook for a Celeborn worker. Asks the worker to decommission when a scale-in is
# removing it, and to shut down gracefully otherwise. See the chart's README.md.
#
# Reads POD_NAME, POD_IP, STS_NAME, WORKER_HTTP_PORT and ALWAYS_DECOMMISSION from the
# environment.

set -u

SA=/var/run/secrets/kubernetes.io/serviceaccount

# Kubernetes counts preStop against terminationGracePeriodSeconds, so no request may hang: a
# server that accepts the connection and then stops responding would burn the whole grace
# period and the pod would be killed without ever draining. Only the wait after a successful
# exit request is allowed to take that long. `-T` is the one timeout flag both busybox and GNU
# wget accept.
HTTP_TIMEOUT=10
CURL_OPTS="--connect-timeout 5 --max-time $HTTP_TIMEOUT"
WGET_OPTS="-T $HTTP_TIMEOUT"

# preStop output is not collected, so log through the main process.
log() {
  _msg="celeborn preStop: $*"
  if [ -w /proc/1/fd/1 ]; then
    echo "$_msg" >/proc/1/fd/1
  else
    echo "$_msg"
  fi
}

ORDINAL=""
DESIRED=""
EXIT_TYPE=GRACEFUL

if [ "${ALWAYS_DECOMMISSION:-false}" = "true" ]; then
  # Graceful shutdown persists state to recover from on restart. Where the worker's storage
  # does not outlive the pod there is nothing to recover, so a rollout would drop in-flight
  # shuffles - drain every time instead, whatever is removing this pod.
  EXIT_TYPE=DECOMMISSION
  log "alwaysDecommission is set - draining regardless of why this pod is going away"
else
  # A scale-in must decommission, waiting for the shuffle data to expire. A rolling update or
  # a node drain must not, or every pod replacement would block for hours. The statefulset
  # controller lowers spec.replicas before deleting pods on a scale-in, so an ordinal at or
  # above the desired count is being removed for good.
  ORDINAL=${POD_NAME:-}
  ORDINAL=${ORDINAL##*-}

  if [ -r "$SA/token" ]; then
    URL="https://kubernetes.default.svc/apis/apps/v1/namespaces/$(cat "$SA/namespace")/statefulsets/${STS_NAME:-}/scale"
    AUTH="Authorization: Bearer $(cat "$SA/token")"
    if command -v curl >/dev/null 2>&1; then
      SCALE=$(curl -sS $CURL_OPTS --cacert "$SA/ca.crt" -H "$AUTH" "$URL" 2>/dev/null)
    else
      SCALE=$(wget -q $WGET_OPTS -O - --ca-certificate="$SA/ca.crt" --header="$AUTH" "$URL" 2>/dev/null)
    fi
    DESIRED=$(printf '%s' "$SCALE" | sed 's/"status".*//' | grep -o '"replicas":[0-9 ]*' | head -n 1 | tr -dc '0-9')
  fi

  case "$ORDINAL" in ''|*[!0-9]*) ORDINAL="" ;; esac
  case "$DESIRED" in ''|*[!0-9]*) DESIRED="" ;; esac

  # Anything unreadable falls back to a graceful shutdown, so a failed lookup cannot stall a
  # rollout with a drain that was never wanted.
  if [ -n "$ORDINAL" ] && [ -n "$DESIRED" ] && [ "$ORDINAL" -ge "$DESIRED" ]; then
    EXIT_TYPE=DECOMMISSION
  fi
fi

if [ "${ALWAYS_DECOMMISSION:-false}" != "true" ] && [ -z "$DESIRED" ]; then
  log "could not read ${STS_NAME:-unknown} desired replicas - a scale-in will NOT decommission"
fi
log "ordinal=${ORDINAL:-unknown} desired=${DESIRED:-unknown} exit=$EXIT_TYPE"

BODY='{"type":"'"$EXIT_TYPE"'"}'

# The exit endpoint hands the drain to a separate thread and answers straight away, so bounding
# the request does not cut a drain short.
# The worker's HTTP server binds to one address, not the wildcard: celeborn.worker.http.host
# defaults to <localhost>, which resolves to this pod's own address, and Jetty is given that
# host. So loopback is refused - target the pod IP, and keep loopback only as a fallback for a
# deployment that has pointed the server somewhere else, such as 0.0.0.0.
HOSTS=""
if [ -n "${POD_IP:-}" ]; then
  case "$POD_IP" in
    *:*) HOSTS="[$POD_IP]" ;;
    *) HOSTS="$POD_IP" ;;
  esac
fi
HOSTS="$HOSTS 127.0.0.1"

RC=1
for HOST in $HOSTS; do
  EXIT_URL="http://$HOST:${WORKER_HTTP_PORT:-9096}/api/v1/workers/exit"
  if command -v curl >/dev/null 2>&1; then
    curl -sS -f $CURL_OPTS -X POST -H 'Content-Type: application/json' -d "$BODY" "$EXIT_URL" >/dev/null 2>&1
    RC=$?
  else
    wget -q $WGET_OPTS -O /dev/null --header='Content-Type: application/json' --post-data="$BODY" "$EXIT_URL"
    RC=$?
  fi
  if [ "$RC" -eq 0 ]; then
    break
  fi
  log "exit request to $HOST failed"
done

if [ "$RC" -eq 0 ]; then
  # Hold the pod open while the worker drains. It exits on its own once done, and
  # terminationGracePeriodSeconds bounds the wait.
  while :; do sleep 5; done
else
  log "exit request failed, falling through to SIGTERM"
fi
