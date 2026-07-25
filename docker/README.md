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
debugging.

## Prerequisites

- Docker (daemon running) and the `docker compose` plugin.
- The repo bundles `build/mvn` used by `make-distribution.sh`. No system Maven
  required, but a JDK is needed to run the build.

## 1. Build the image

```bash
./docker/build-image.sh
# Add a client profile to also bundle a Spark/Flink/etc. client, e.g.:
# ./docker/build-image.sh -Pspark-3.5
```

This runs `./build/make-distribution.sh` (producing `dist/`) and then
`docker build -t celeborn:dev -f dist/docker/Dockerfile dist/`.

## 2. Start the cluster

```bash
docker compose -f docker/docker-compose.yaml up -d
docker compose -f docker/docker-compose.yaml ps
```

The master has a healthcheck; the 3 workers start once the master is healthy
and register with it.

## 3. Verify

```bash
# Master HTTP (expect HTTP 200)
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9098/metrics/json

# Tail logs; look for "Registered worker" on the master
docker compose -f docker/docker-compose.yaml logs -f celeborn-master
```

## 4. Stop

```bash
docker compose -f docker/docker-compose.yaml down
```

`down` removes the containers, so worker shuffle data (in the containers'
writable layer) is discarded. `restart` keeps it.
