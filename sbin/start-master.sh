#!/usr/bin/env bash

# Starts the rss master on the machine this script is executed on.

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

if [ "$RSS_MASTER_MEMORY" = "" ]; then
  RSS_MASTER_MEMORY="1g"
fi

export RSS_JAVA_OPTS="-Xmx$RSS_MASTER_MEMORY $RSS_MASTER_JAVA_OPTS"

"${RSS_HOME}/sbin/rss-daemon.sh" start com.aliyun.emr.rss.service.deploy.master.Master 1 "$@"