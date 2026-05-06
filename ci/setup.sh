#!/bin/bash

set +x

# Install the shared JDK/tooling once per step so individual CI scripts stay small.
setup_jdk() {
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jdk
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
}

# Normalize Buildkite's secret shape into the names Maven and sbt publish expect.
setup_artifactory_auth() {
  if [[ -z "${DATA_PLATFORM_ARTIFACTORY_TOKEN:-}" ]]; then
    return
  fi

  export ARTIFACTORY_USER="admin"
  export ARTIFACTORY_TOKEN="${DATA_PLATFORM_ARTIFACTORY_TOKEN#*=}"
}

# Read the Maven base version, then derive the immutable OpenAI version for this step.
setup_version() {
  local base_version
  local chart_base_version
  local snapshot_suffix=""

  base_version="$(build/mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n 1)"
  if [[ ! "${base_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "project.version must be a regular x.y.z version, got ${base_version}" >&2
    return 1
  fi

  chart_base_version="$(sed -n 's/^version:[[:space:]]*//p' charts/celeborn/Chart.yaml | head -n 1)"
  if [[ ! "${chart_base_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "charts/celeborn/Chart.yaml version must be a regular x.y.z version, got ${chart_base_version}" >&2
    return 1
  fi

  if [[ ! "${BUILDKITE_BRANCH}" =~ ^branch-.*-openai$ ]]; then
    snapshot_suffix="-SNAPSHOT"
  fi

  # Keep the SemVer numeric core as x.y.z, then put BUILDKITE_BUILD_NUMBER in the suffix.
  export VERSION="${base_version}-openai-${BUILDKITE_BUILD_NUMBER}.${BUILDKITE_COMMIT}${snapshot_suffix}"
  export HELM_CHART_VERSION="${chart_base_version}-openai-${BUILDKITE_BUILD_NUMBER}.${BUILDKITE_COMMIT}${snapshot_suffix}"
}

# Set Maven defaults shared by CI steps.
setup_maven() {
  export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=1g"
}

setup_jdk
setup_artifactory_auth
setup_version
setup_maven
