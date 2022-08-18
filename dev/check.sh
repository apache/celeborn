#!/usr/bin/env bash

# usage: check.sh <maven build options>

MVN_PROFILE="$@"

RSS_HOME="$(cd "`dirname "$0"`/.."; pwd)"
MVN="$RSS_HOME/build/mvn"

$MVN $MVN_PROFILE clean
$MVN $MVN_PROFILE install -DskipTests
$MVN $MVN_PROFILE checkstyle:check
$MVN $MVN_PROFILE scalastyle:check
