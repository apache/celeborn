#!/usr/bin/env bash

# Stops the rss master on the machine this script is executed on.

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

"${RSS_HOME}/sbin/rss-daemon.sh" stop com.aliyun.emr.rss.service.deploy.master.Master 1