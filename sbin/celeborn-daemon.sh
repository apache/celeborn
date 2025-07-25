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

# Runs a celeborn command as a daemon.
#
# Environment Variables
#
#   CELEBORN_CONF_DIR  Alternate conf dir. Default is ${CELEBORN_HOME}/conf.
#   CELEBORN_LOG_DIR   Where log files are stored. ${CELEBORN_HOME}/logs by default.
#   CELEBORN_PID_DIR   The pid files are stored. /tmp by default.
#   CELEBORN_IDENT_STRING   A string representing this instance of celeborn. $USER by default
#   CELEBORN_NICENESS The scheduling priority for daemons. Defaults to 0.
#   CELEBORN_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
##

usage() {
  echo "Usage: celeborn-daemon.sh [--config <conf-dir>] (start|stop|restart|status) <celeborn-class-name> <celeborn-instance-number> [args]"
  echo ""
  echo "Options:"
  echo "  <conf-dir>                  Override CELEBORN_CONF_DIR"
  echo "  <celeborn-class-name>       The main class name of the celeborn command to run"
  echo "                              Example: org.apache.celeborn.service.deploy.master.Master for celeborn master"
  echo "                                       org.apache.celeborn.service.deploy.worker.Worker for celeborn worker"
  echo "  <celeborn-instance-number>  The instance number of the celeborn command to run"
}

# if no args specified, show usage
if [ $# -le 1 ]; then
  usage
  exit 1
fi

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.

if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    usage
    exit 1
  else
    export CELEBORN_CONF_DIR="$conf_dir"
  fi
  shift
fi

option=$1
shift
command=$1
shift
instance=$1
shift

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"

celeborn_rotate_log() {
    log=$1;
    num=5;
    if [ -n "$2" ]; then
      num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
      while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
        num=$prev
      done
      mv "$log" "$log.$num";
    fi
}

if [ "$CELEBORN_IDENT_STRING" = "" ]; then
  export CELEBORN_IDENT_STRING="$USER"
fi

# some variables
log="$CELEBORN_LOG_DIR/celeborn-$CELEBORN_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$CELEBORN_PID_DIR/celeborn-$CELEBORN_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$CELEBORN_NICENESS" = "" ]; then
    export CELEBORN_NICENESS=0
fi

execute_command() {
  if [ -z ${CELEBORN_NO_DAEMONIZE+set} ]; then
      exec nohup -- "$@" >> $log 2>&1 < /dev/null &
      newpid="$!"

      echo "$newpid" > "$pid"

      # Poll for up to 5 seconds for the java process to start
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]] || [[ $(ps -p "$newpid" -o comm=) =~ "jboot" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]] && [[ ! $(ps -p "$newpid" -o comm=) =~ "jboot" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      exec "$@"
  fi
}

run_command() {
  mode="$1"
  shift

  mkdir -p "$CELEBORN_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  celeborn_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      execute_command nice -n "$CELEBORN_NICENESS" "${CELEBORN_HOME}"/bin/celeborn-class "$command" "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

start_celeborn() {
    if [ ! -d "$CELEBORN_CONF_DIR" ]
    then
      echo "ERROR : CELEBORN_CONF_DIR: $CELEBORN_CONF_DIR is not a directory"
      exit 1
    fi
    run_command class "$@"
}

stop_celeborn() {
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID"
        wait_time=0
        # keep same with `celeborn.worker.graceful.shutdown.timeout`
        wait_timeout=600
        while [[ $(ps -p "$TARGET_ID" -o comm=) != "" && $wait_time -lt $wait_timeout ]];
        do
          sleep 1
          ((wait_time++))
          echo "waiting for server shutdown, wait for ${wait_time}s"
        done

        if [[ $(ps -p "$TARGET_ID" -o comm=) == "" ]]; then
          rm -f "$pid"
        else
          echo "Failed to stop server(pid=$TARGET_ID) after ${wait_timeout}s"
          exit 1
        fi
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
}

check_celeborn(){
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "$command is running."
        exit 0
      else
        echo "$pid file is present but $command not running"
        exit 1
      fi
    else
      echo "$command not running."
      exit 2
    fi
}

case $option in

  (start)
    start_celeborn "$@"
    ;;

  (stop)
    stop_celeborn
    ;;

  (restart)
    echo "Restarting Celeborn"
    stop_celeborn
    start_celeborn "$@"
    ;;

  (status)
    check_celeborn
    ;;

  (*)
    usage
    exit 1
    ;;

esac
