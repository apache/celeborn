#!/usr/bin/env bash

# Stops the ess master on the machine this script is executed on.

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${CELEBORN_HOME}/sbin/celeborn-daemon.sh" stop org.apache.celeborn.verifier.runner.Runner "$WORKER_INSTANCE"