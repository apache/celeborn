#!/usr/bin/env bash

# Starts the rss worker on the machine this script is executed on.

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

if [ "$RSS_WORKER_MEMORY" = "" ]; then
  RSS_WORKER_MEMORY="1g"
fi

if [ "$RSS_WORKER_OFFHEAP_MEMORY" = "" ]; then
  RSS_WORKER_OFFHEAP_MEMORY="1g"
fi

export RSS_JAVA_OPTS="-Xmx$RSS_WORKER_MEMORY -XX:MaxDirectMemorySize=$RSS_WORKER_OFFHEAP_MEMORY $RSS_WORKER_JAVA_OPTS"

if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${RSS_HOME}/sbin/rss-daemon.sh" start com.aliyun.emr.rss.service.deploy.worker.Worker "$WORKER_INSTANCE" "$@"