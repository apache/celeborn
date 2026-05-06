#!/bin/bash

set -euo pipefail

# Set up Java, Maven, Artifactory credentials, and the immutable version.
source ci/setup.sh

# Publish only the OpenAI-supported shaded Spark client jars.
publish_shaded_jar() {
  local profile="$1"
  local project="$2"

  SBT_MAVEN_PROFILES="${profile}" \
    build/sbt "-P${profile}" \
    "${project}/publish"
}

echo "Publishing Celeborn shaded jars as ${VERSION}"
publish_shaded_jar "spark-3.5" "celeborn-client-spark-3-shaded"
publish_shaded_jar "spark-4.0" "celeborn-client-spark-4-shaded"
