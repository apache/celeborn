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

# Build the local Celeborn docker image (celeborn:dev) for docker-compose.
#
# Two steps:
#   1. ./build/make-distribution.sh   -> produces dist/ (bin/sbin/conf/jars/...
#      plus dist/docker/Dockerfile copied by the distribution script itself)
#   2. docker build                   -> builds the image with context = dist/
#      (the Dockerfile COPYs bin/sbin/... relative to the context root)
#
# The image is NOT built via the compose `build:` key, because make-distribution
# must run before docker build and dist/ may not exist yet when invoking
# `docker compose up`.
#
# Usage:
#   ./docker/build-image.sh                      # core master/worker/cli
#   ./docker/build-image.sh -Pspark-3.5          # also build Spark client
#   CELEBORN_IMAGE_TAG=celeborn:dev ./docker/build-image.sh
#
# Requirements: docker on PATH; build/mvn is bundled in the repo.

set -euo pipefail

# Resolve repo root (parent of this script's directory).
cd "$(dirname "$0")/.."

IMAGE_TAG="${CELEBORN_IMAGE_TAG:-celeborn:dev}"

echo "==> Building distribution (./build/make-distribution.sh $*) ..."
./build/make-distribution.sh "$@"

echo "==> Building docker image ${IMAGE_TAG} from dist/ ..."
if [ ! -f dist/docker/Dockerfile ]; then
  echo "ERROR: dist/docker/Dockerfile not found. make-distribution may have failed." >&2
  exit 1
fi
docker build -t "${IMAGE_TAG}" -f dist/docker/Dockerfile dist/

echo "==> Done. Image: ${IMAGE_TAG}"
echo "Start the cluster:"
echo "  docker compose -f docker/docker-compose.yaml up -d"
