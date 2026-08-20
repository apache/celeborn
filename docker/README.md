<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Celeborn Local Cluster (docker-compose)

A local Celeborn cluster — 1 master (HA off) + 3 workers — for development and
debugging, optionally accompanied by an example Spark cluster (1 master + 2
workers) whose shuffle data is served by Celeborn, so the whole stack can be
exercised end-to-end.

## Prerequisites

- Docker (daemon running) and the `docker compose` plugin.
- The repo bundles `build/mvn` used by `make-distribution.sh`. No system Maven
  required, but a JDK is needed to run the build.

## 1. Build the images

```bash
# Celeborn only (no Spark example cluster):
./docker/build-image.sh

# Also build the Spark example image (celeborn-spark:dev):
./docker/build-image.sh -Pspark-3.5    # Spark 3 client
./docker/build-image.sh -Pspark-4.0    # or Spark 4 client
```

This runs `./build/make-distribution.sh` (producing `dist/`) and then:

- `docker build -t celeborn:dev -f dist/docker/Dockerfile dist/` — the
  Celeborn master/worker image, always built.
- If a Spark client profile was passed (e.g. `-Pspark-3.5` or `-Pspark-4.0`),
  the distribution also produces `dist/spark/celeborn-client-spark-*-shaded_*.jar`;
  `build-image.sh` copies it into `docker/spark/` and runs
  `docker build -t celeborn-spark:dev docker/spark/` — a Spark image with the
  Celeborn client jar baked in. Without a Spark profile, the Spark image is
  skipped and only the Celeborn cluster is available.

## 2. Start the cluster

```bash
docker compose -f docker/docker-compose.yaml up -d
docker compose -f docker/docker-compose.yaml ps
```

The Celeborn master has a healthcheck; the 3 workers start once the master is
healthy and register with it. When `celeborn-spark:dev` is present, the Spark
master and 2 workers start after the Celeborn master is healthy.

## 3. Verify

```bash
# Master HTTP (expect HTTP 200)
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9098/metrics/json

# Tail logs; look for "Registered worker" on the master
docker compose -f docker/docker-compose.yaml logs -f celeborn-master
```

## 4. Run a Spark job that shuffles through Celeborn

Spark is pre-wired to use Celeborn via `docker/spark/conf/spark-defaults.conf`
(`spark.shuffle.manager=...CelebornShuffleManager`, master endpoints
`celeborn-master:9097`), so no extra `--conf` flags are needed. Submit Spark's
built-in `GroupByTest`, which forces a shuffle:

```bash
docker exec celeborn-spark-master /opt/spark/bin/run-example \
  --master spark://celeborn-spark-master:7077 \
  GroupByTest 10 100 1000 10
```

The job should finish with `final status: SUCCEEDED`. While it runs, watch the
shuffle traffic land on Celeborn workers (push/fetch requests) and confirm the
shuffle really went through Celeborn rather than the local Spark shuffle:

```bash
docker compose -f docker/docker-compose.yaml logs -f celeborn-worker
# Master HTTP metrics: the worker push/fetch counters should increase.
curl -s http://localhost:9098/metrics/json | grep -i shuffle
```

The Spark master Web UI is at http://localhost:8080/.

## 5. Stop

```bash
docker compose -f docker/docker-compose.yaml down
```

`down` removes the containers (Celeborn and Spark) and the `shared` volume, so
worker shuffle data (in the containers' writable layer) is discarded.
`restart` keeps it.
