#!/usr/bin/env bash

# Starts the ess master on the machine this script is executed on.

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"

if [ "$CELEBORN_VERF_SCHEDULER_MEMORY" = "" ]; then
  CELEBORN_VERF_SCHEDULER_MEMORY="1g"
fi

export CELEBORN_JAVA_OPTS="-Xmx$CELEBORN_VERF_SCHEDULER_MEMORY $CELEBORN_VERF_SCHEDULER_JAVA_OPTS"

"${CELEBORN_HOME}/sbin/celeborn-daemon.sh" start org.apache.celeborn.verifier.scheduler.Scheduler 1 "$@"