#!/usr/bin/env bash

# Runs a RSS command as a daemon.
#
# Environment Variables
#
#   RSS_CONF_DIR  Alternate conf dir. Default is ${RSS_HOME}/conf.
#   RSS_LOG_DIR   Where log files are stored. ${RSS_HOME}/logs by default.
#   RSS_PID_DIR   The pid files are stored. /tmp by default.
#   RSS_IDENT_STRING   A string representing this instance of rss. $USER by default
#   RSS_NICENESS The scheduling priority for daemons. Defaults to 0.
#   RSS_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
##

usage="Usage: rss-daemon.sh [--config <conf-dir>] (start|stop|status) <rss-command> <rss-instance-number> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${RSS_HOME}/sbin/rss-config.sh"

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
    echo $usage
    exit 1
  else
    export RSS_CONF_DIR="$conf_dir"
  fi
  shift
fi

option=$1
shift
command=$1
shift
instance=$1
shift

rss_rotate_log ()
{
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

if [ "$RSS_IDENT_STRING" = "" ]; then
  export RSS_IDENT_STRING="$USER"
fi

export RSS_PRINT_LAUNCH_COMMAND="1"

# get log directory
if [ "$RSS_LOG_DIR" = "" ]; then
  export RSS_LOG_DIR="${RSS_HOME}/logs"
fi
mkdir -p "$RSS_LOG_DIR"
touch "$RSS_LOG_DIR"/.rss_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$RSS_LOG_DIR"/.rss_test
else
  chown "$RSS_IDENT_STRING" "$RSS_LOG_DIR"
fi

if [ "$RSS_PID_DIR" = "" ]; then
  RSS_PID_DIR="${RSS_HOME}/pids"
fi

# some variables
log="$RSS_LOG_DIR/rss-$RSS_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$RSS_PID_DIR/rss-$RSS_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
if [ "$RSS_NICENESS" = "" ]; then
    export RSS_NICENESS=0
fi

execute_command() {
  if [ -z ${RSS_NO_DAEMONIZE+set} ]; then
      nohup -- "$@" >> $log 2>&1 < /dev/null &
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
      "$@"
  fi
}

run_command() {
  mode="$1"
  shift

  mkdir -p "$RSS_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  rss_rotate_log "$log"
  echo "starting $command, logging to $log"

  case "$mode" in
    (class)
      execute_command nice -n "$RSS_NICENESS" "${RSS_HOME}"/bin/rss-class "$command" "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in

  (start)
    run_command class "$@"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (restart)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
        wait_time=0
        while [[ $(ps -p "$TARGET_ID" -o comm=) != "" ]];
        do
          sleep 1s
          ((wait_time++))
          echo "waiting for worker graceful shutdown, wait for ${wait_time}s"
        done
        run_command class "$@"
      else
        rm -f "$pid"
        echo "no $command to stop, directly start"
        run_command class "$@"
      fi
    else
      echo "no $command to stop, directly start"
      run_command class "$@"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
