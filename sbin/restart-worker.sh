#!/usr/bin/env bash

# Restart the rss worker on the machine this script is executed on.

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${RSS_HOME}/sbin/rss-daemon.sh" restart com.aliyun.emr.rss.service.deploy.worker.Worker "$WORKER_INSTANCE"