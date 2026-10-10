<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Helm Chart for Apache Celeborn

[Apache Celeborn](https://celeborn.apache.org) is an intermediate data service for Big Data compute engines (i.e. ETL, OLAP and Streaming engines) to boost performance, stability, and flexibility. Intermediate data typically include shuffle and spilled data.

## Introduction

This chart will bootstrap an [Celeborn](https://celeborn.apache.org) deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Requirements

Configure kubectl to connect to the Kubernetes cluster.

- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl)
- [Helm 3.0+](https://helm.sh/docs/using_helm/#installing-helm)

## Template rendering

When you want to test the template rendering, but not actually install anything. [Debugging templates](https://helm.sh/docs/chart_template_guide/debugging/) provide a quick way of viewing the generated content without YAML parse errors blocking.

There are two ways to render templates. It will return the rendered template to you so you can see the output.

- Local rendering chart templates

```shell
helm template --debug ../celeborn
```

- Server side rendering chart templates

```shell
helm install --dry-run --debug --generate-name ../celeborn
```

More details in [Helm Install](https://helm.sh/docs/helm/helm_install/).
The chart can be customized using the following [celeborn configurations](https://celeborn.apache.org/docs/latest/configuration/#important-configurations).
Specify parameters using `--set key=value[,key=value]` argument to `helm install`.

## Health probes

Both the master and the worker serve `GET /healthz` on their HTTP port, returning `200` when the
process can serve and `503` when it cannot. The chart wires probes to that endpoint through
`master.startupProbe`, `master.livenessProbe`, `master.readinessProbe` and the matching `worker.*`
values. Each value is a complete Kubernetes probe object, so it can be tuned freely or set to
`null` to drop the probe.

The defaults are deliberately asymmetric:

- The master check is shallow - it reports healthy once the master HTTP service is up - so a
  startup and a liveness probe are enabled by default. The startup probe holds off the liveness
  probe while a master waits for the DNS records of its peers, which can take a while if one
  replica is slow to be scheduled.
- The worker readiness check is enabled by default and is the reason the probes exist: it reports
  healthy only after the worker has finished initializing and registered with the master, which
  stops a rolling update from moving on to the next worker while the previous one still rejects
  traffic.
- The worker liveness probe is disabled by default. Worker `/healthz` fails while the worker is
  not registered with the master, so pointing a liveness probe at it would restart every worker in
  the cluster during a master outage or failover. If a worker liveness probe is wanted, prefer a
  `tcpSocket` check on the RPC port.
- The master readiness probe is disabled by default, because it deadlocks a cold start under the
  default pod management policy - see below.

The master startup probe allows `failureThreshold` x `periodSeconds` - 300s by default - for the
peers to resolve before the container is restarted. Raise `failureThreshold` on clusters where
scheduling a master replica routinely takes longer than that.

### Parked workers and rolling updates

A worker parked with `DecommissionThenIdle` reports state `Idle`, which `/healthz` treats as not
serving, because the master does not schedule to an `Idle` worker either. Its pod therefore stays
NotReady and a rolling update will not advance past that ordinal. Recommission parked workers
before upgrading, or set `worker.readinessProbe: null` for that rollout.

### Readiness and pod management policy

Both statefulsets leave `podManagementPolicy` unset, which means `OrderedReady`: no replica is
created until the previous one is ready. A master does not start serving until the DNS records of
all master replicas resolve, so a ready-gated master-0 would wait for a master-1 that the
statefulset controller will not create until master-0 is ready. Enabling `master.readinessProbe`
therefore requires `master.podManagementPolicy: Parallel`.

Worker readiness has no such cycle - workers wait on master DNS, not on each other - but under
`OrderedReady` it does serialize a cold start, since each worker is created only after the
previous one has registered. Set `worker.podManagementPolicy: Parallel` to start them all at
once; rolling updates stay gated by readiness either way.

`podManagementPolicy` is immutable. Changing it on an existing release makes `helm upgrade` fail,
and the statefulset has to be deleted with `--cascade=orphan` and re-applied first.

Both headless services set `publishNotReadyAddresses: true`. For the master service this is
required rather than cosmetic: a headless service only publishes the per-pod DNS record of a pod
once it is ready, and both the startup wait and `celeborn.master.endpoints` address masters by
those per-pod names, so a ready-gated master would withhold the very record its peers wait for.
The worker service sets it to keep DNS and endpoints behaving as they did before readiness was
gated, so that a worker recovering from deregistration stays addressable.

## Documentation

For additional details on deploying the Celeborn Kubernetes Helm chart, please refer to the [Celeborn on Kubernetes](https://celeborn.apache.org/docs/latest/deploy_on_k8s/) documentation.
