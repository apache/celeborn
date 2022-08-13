#!/usr/bin/env bash

RSS_HOME="$(cd "`dirname "$0"`/.."; pwd)"

$RSS_HOME/dev/check.sh -Pspark-2.4 -Plog4j-1
