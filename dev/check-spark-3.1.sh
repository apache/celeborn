#!/usr/bin/env bash

RSS_HOME="$(cd "`dirname "$0"`/.."; pwd)"

$RSS_HOME/dev/check.sh -Pspark-3.1 -Plog4j-1
