#!/usr/bin/env bash

# Starts the ess worker on the machine this script is executed on.

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"

if [ "$CELEBORN_VERF_CLI_MEMORY" = "" ]; then
  CELEBORN_VERF_CLI_MEMORY="1g"
fi


export CELEBORN_JAVA_OPTS="-Xmx$CELEBORN_VERF_CLI_MEMORY  $CELEBORN_VERF_CLI_JAVA_OPTS"

if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${CELEBORN_HOME}/sbin/celeborn-daemon.sh" start org.apache.celeborn.verifier.cli.Cli "$WORKER_INSTANCE" "$@"