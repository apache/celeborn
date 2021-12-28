#!/usr/bin/env bash

# Starts the rss master on the machine this script is executed on.

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

if [ -f "${RSS_CONF_DIR}/hosts" ]; then
  HOST_LIST=$(awk '/\[/{prefix=$0; next} $1{print prefix,$0}' "${RSS_CONF_DIR}/hosts")
else
  HOST_LIST="[master] localhost\n[worker] localhost"
fi

# By default disable strict host key checking
if [ "$RSS_SSH_OPTS" = "" ]; then
  RSS_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

# start masters
for host in `echo "$HOST_LIST" | sed  "s/#.*$//;/^$/d" | grep '\[master\]' | awk '{print $NF}'`
do
  if [ -n "${RSS_SSH_FOREGROUND}" ]; then
    ssh $RSS_SSH_OPTS "$host" "${RSS_HOME}/sbin/stop-master.sh"
  else
    ssh $RSS_SSH_OPTS "$host" "${RSS_HOME}/sbin/stop-master.sh" &
  fi
  if [ "$RSS_SLEEP" != "" ]; then
    sleep $RSS_SLEEP
  fi
done

# start workers
for host in `echo "$HOST_LIST"| sed  "s/#.*$//;/^$/d" | grep '\[worker\]' | awk '{print $NF}'`
do
  if [ -n "${RSS_SSH_FOREGROUND}" ]; then
    ssh $RSS_SSH_OPTS "$host" "${RSS_HOME}/sbin/stop-worker.sh"
  else
    ssh $RSS_SSH_OPTS "$host" "${RSS_HOME}/sbin/stop-worker.sh" &
  fi
  if [ "$RSS_SLEEP" != "" ]; then
    sleep $RSS_SLEEP
  fi
done

wait