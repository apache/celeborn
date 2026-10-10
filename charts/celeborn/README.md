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

## Zone-aware worker replication

By default the chart deploys all workers in a single statefulset, and which zone a worker
lands in is whatever the scheduler decides. Pod-level spreading - `worker.affinity`, or a
topology spread constraint where the chart supports one - can balance that, but the balance is
re-derived on every scheduling decision: it holds while the constraint is satisfiable and
silently degrades when it is not, for example when one zone is out of capacity for the instance
type the workers need, or when several nodes are replaced at once.

Setting `worker.zoneAwareReplication.enabled` deploys one worker statefulset per zone
instead, which makes zone membership structural rather than emergent:

```yaml
worker:
  # Total across all zones; each zone gets ceil(replicas / zones) = 3.
  replicas: 9
  zoneAwareReplication:
    enabled: true
    zones:
      - name: us-east-1a
        nodeSelector:
          topology.kubernetes.io/zone: us-east-1a
      - name: us-east-1b
        nodeSelector:
          topology.kubernetes.io/zone: us-east-1b
      - name: us-east-1c
        nodeSelector:
          topology.kubernetes.io/zone: us-east-1c
```

This renders `celeborn-worker-us-east-1a`, `-1b` and `-1c`, each carrying a
`celeborn.apache.org/zone` label that is part of its statefulset selector. What it gives you:

- A zone's worker count is declared, not inferred, so it survives concurrent node churn and
  a full recreate.
- Each zone can be rolled, scaled or paused on its own, leaving the other zones serving.
- A worker's ordinal-to-zone mapping is stable, so its identity
  (`<pod>.<service>.<namespace>.svc.<cluster>.local`) implies its zone. That is what makes
  zone-pinned worker selection (see below) predictable.

Notes:

- `worker.replicas` is the total across zones; each zone deploys
  `ceil(worker.replicas / number of zones)`, so 5 replicas over 3 zones is 2 per zone (6
  total). Set `zones[].replicas` to size a zone explicitly, which is also the lever for
  running a zone short while its capacity is constrained.
- `zones[].nodeSelector` is merged over `worker.nodeSelector` and wins on conflicting keys;
  `zones[].affinity` replaces `worker.affinity` for that zone.
- All zones share one headless service, so the service and the pod monitor keep selecting
  workers in every zone. Only the statefulset selectors are zone-scoped.
- Per-zone `nodeSelector` already separates the zones physically, so a zone
  `topologySpreadConstraints` entry becomes redundant once this is enabled. A per-hostname
  spread or anti-affinity rule is still meaningful within a zone.

### Zone-aware replication is not zone-aware data placement

This only controls where worker pods run. It does not make a client prefer a worker in its
own zone: Celeborn allocates slots in the master, and the client-side view of which workers
are eligible comes from worker tags (`celeborn.tags.tagsExpr`, `celeborn.tags.enabled`), not
from pod topology. To keep shuffle traffic inside one zone, tag each worker with its zone and
have each application select its own zone's tag. Note that
`celeborn.client.reserveSlots.rackaware.enabled` does the opposite on purpose: it spreads a
partition's replicas across racks for durability.

### Migrating an existing release

The statefulset names change, so enabling this on a running release replaces
`<fullname>-worker` with the per-zone statefulsets. Every worker is recreated, and with it
its identity, so:

- Worker data on `hostPath` or `emptyDir` volumes does not survive.
- The master holds stale worker registrations until `celeborn.master.heartbeat.worker.timeout`
  elapses.
- In-flight shuffles fail.

Treat it as a maintenance-window change rather than a rolling update.

## Draining a worker before it is removed

Deleting a worker pod - by scaling in, rolling out, or draining its node - reaches Celeborn as
a `SIGTERM`. That never enters the decommission path, so the worker leaves with its shuffle data
and any application still reading from it loses that stage. Celeborn's safe drain is
`DECOMMISSION`, which waits for the worker's shuffle keys to expire (up to
`celeborn.worker.decommission.forceExitTimeout`, 6h by default) and is only reachable through
the worker's HTTP API.

This is worth having with a fixed replica count, not only with `worker.autoscaling` - any
rollout removes workers too.

`worker.drain.enabled` adds a `preStop` hook that calls it. The hook runs `files/worker-drain.sh`, shipped in the chart's
config map and mounted at `/opt/celeborn/drain/worker-drain.sh`; it takes `POD_NAME`,
`STS_NAME` and `WORKER_HTTP_PORT` from the environment, so the script itself is the same in
every zone. The hook must not decommission on a rolling update or a node
drain, or every pod replacement would block for hours, so it distinguishes the two: the
statefulset controller lowers `spec.replicas` *before* deleting pods on a scale-in, so a pod
whose ordinal is at or above the desired count is being removed for good and decommissions,
while any other pod shuts down gracefully and comes back. If the desired count cannot be read
the hook falls back to a graceful shutdown, so a broken lookup cannot stall a rollout.

This needs three things:

- `rbac.create: true`. The chart adds `get` on `statefulsets/scale` to the role when the
  drain is enabled; the hook reads the count with the pod's own service account.
- `celeborn.worker.graceful.shutdown.enabled: true`, so the non-scale-in path actually
  persists state and recovers on restart.
- `worker.terminationGracePeriodSeconds` above `celeborn.worker.decommission.forceExitTimeout`.
  This is an upper bound, not a wait: a graceful shutdown still finishes in
  `celeborn.worker.graceful.shutdown.timeout`, so a long grace period does not slow rollouts
  down.

It is off by default. Turn it on alongside `worker.autoscaling`, or leave it off and treat
scale-in as destructive, only letting it happen when the fleet is idle.

### When the worker's storage does not outlive the pod

Telling a scale-in from a rollout is only worth doing if a rollout can recover. Graceful
shutdown persists committed file metadata to `celeborn.worker.graceful.shutdown.recoverPath` and
recovers from it when the worker comes back - which needs both that path and
`celeborn.worker.storage.dirs` to survive the pod. On an `emptyDir`, or on instance-store disks
that are wiped when the node is replaced, neither does: the worker returns to empty disks, there
is nothing to recover, and the in-flight shuffles it was holding are lost.

`worker.drain.alwaysDecommission: true` drops the distinction and decommissions on
every termination, so a worker is never removed while an application still needs what is on it.
It also needs no Kubernetes API access, since it never reads the replica count, and the chart
leaves the `statefulsets/scale` rule out of the role.

What it costs:

- Rollouts and node drains wait for the drain as well, bounded by
  `celeborn.worker.decommission.forceExitTimeout` rather than
  `celeborn.worker.graceful.shutdown.timeout`. With `OrderedReady` a rollout drains each worker
  in turn, so budget accordingly.
- Anything that evicts a pod on a deadline shorter than the drain will still kill it mid-drain -
  a cluster autoscaler reclaiming a node, or a spot interruption with its two-minute notice.
  Make sure the node pool's own grace period is at least as long as you expect a drain to take,
  or accept that those paths behave as they did before.

#### Worked example: local NVMe through an `emptyDir`

A common shape for workers is to skip network storage entirely and write to the node's own NVMe
- an `emptyDir` mounted at `celeborn.worker.storage.dirs`, landing on the instance's local disks.
On AWS that means instance-store NVMe, and on a node OS that assembles those disks into one
array for container storage (Bottlerocket's local-storage setup, for instance) an `emptyDir`
lands on that array with no extra wiring. It is a reasonable choice: shuffle is a
write-heavy, short-lived, reproducible workload, so there is little reason to pay for network
volumes, per-volume throughput ceilings, or PVCs that pin an ordinal to one zone.

The catch is lifetime, and it is shorter than people expect. An `emptyDir` is deleted when the
pod leaves the node - not only when the node goes away. So both
`celeborn.worker.storage.dirs` and `celeborn.worker.graceful.shutdown.recoverPath`, if it sits
under the same mount, disappear on every pod replacement. Graceful shutdown still writes its
recovery database, and the replacement worker still starts up and re-registers, but it comes
back to empty disks and there is nothing to read the database against.

What that costs while jobs are running: a rolling update, a chart upgrade or a scale-in removes
a worker that is holding shuffle data for applications still running. Those fetches fail, and
since `celeborn.client.push.replicate.enabled` is `false` by default there is no second copy -
the engine recomputes the stages that produced the lost partitions. A rollout across a fleet of
workers can do this repeatedly, to whatever is running at the time.

So on ephemeral storage the drain is what keeps a running job's shuffle alive, and
`alwaysDecommission` is the setting that matters - the scale-in/rollout distinction is pointless
when neither can recover:

```yaml
worker:
  # Above celeborn.worker.decommission.forceExitTimeout (6h), so kubelet does not kill a
  # worker part-way through draining.
  terminationGracePeriodSeconds: 21900
  drain:
    enabled: true
    alwaysDecommission: true
  volumes:
    - name: celeborn-data
      emptyDir: {}
  volumeMounts:
    - name: celeborn-data
      mountPath: /celeborn-data/disk0
celeborn:
  celeborn.worker.storage.dirs: /celeborn-data/disk0:disktype=SSD
  celeborn.worker.graceful.shutdown.enabled: true
```

A worker then stays up until the applications holding data on it are finished, and only then
exits - so no running job loses its shuffle to a deployment.

Note that an `emptyDir` needs no `chown` init container, unlike the `hostPath` volumes the chart
mounts by default: Kubernetes applies `worker.podSecurityContext.fsGroup` to it, so the worker
can write to the mount as it is.

One thing to expect when first enabling the drain: the script is shipped in the config map that
master and worker share, so adding it changes the `celeborn.apache.org/conf-hash` annotation on
both and the masters roll as well.

Two things this still does not cover. Losing the node itself - a spot interruption, or a node
autoscaler consolidating - destroys the disks whatever the pod does, and gives less time than a
drain needs; set the node pool's grace period to at least the drain you expect, and treat the
remainder as accepted risk. And if you want a rollout to genuinely recover rather than wait,
storage has to outlive the pod: a `hostPath` on the NVMe mount for both `storage.dirs` and
`recoverPath` survives a pod restart on the same node, though nothing survives the node being
replaced, and a `hostPath` gives the scheduler no reason to put the pod back where its data is.

## Autoscaling workers with KEDA

`worker.autoscaling` creates a [KEDA](https://keda.sh) `ScaledObject` for each worker
statefulset. With zone-aware replication enabled that is one per zone, so each zone scales
on its own load - which is what you want when applications are pinned to a zone, because
their load is genuinely uneven across zones.

The chart ships two triggers - disk and off-heap memory, the two resources whose exhaustion
takes a worker out of service - both querying metrics the worker already exports. Point them at
a Prometheus-compatible endpoint and they work as they are:

```yaml
worker:
  terminationGracePeriodSeconds: 21900   # see "Draining a worker before it is removed"
  drain:
    enabled: true
  autoscaling:
    enabled: true
    prometheusAddress: http://prometheus.monitoring.svc.cluster.local:9090
    maxReplicaCount: 6
    behavior:
      scaleDown:
        stabilizationWindowSeconds: 1800
        policies:
          - type: Pods
            value: 1
            periodSeconds: 900
```

**Disk usage** is the signal that bites first, since a worker that fills up stops accepting
pushes. It scales on the fullest worker in the statefulset, as a fraction of Celeborn disk
capacity:

```promql
max((1 - metrics_DeviceCelebornFreeBytes_Value{...} / metrics_DeviceCelebornTotalBytes_Value{...})
    and on (instance) metrics_IsDecommissioningWorker_Value{...} == 0)
```

It uses `metricType: Value`, because a ratio must not be divided across replicas.

This trigger is also the slot utilization target, which is why there is no separate one for
slots. Celeborn sizes a disk's slot capacity from its space -
`maxSlots = totalSpace / estimatedPartitionSize` in `WorkerInfo` - so slots and bytes fill in
step, and `activeSlots / maxSlots` reduces to the disk fraction above. A slot percentage could
not be computed anyway: `maxSlots` lives in the master's `DiskInfo` and no metric exports it.
If you want the raw count as a leading signal - slots are allocated when a stage starts, before
its data is written - add it yourself through `worker.autoscaling.triggers` using
`sum(metrics_ActiveSlotsCount_Value{role="Worker"})` with `metricType: AverageValue` and a
per-worker target.

**Off-heap memory** is the other way a worker takes itself out of service: it stops accepting
pushes at `celeborn.worker.directMemoryRatioToPauseReceive` (0.85) and replication at 0.95.
`DirectMemoryUsageRatio` is `memoryUsage / maxDirectMemory`, so it is already a fraction of
`CELEBORN_WORKER_OFFHEAP_MEMORY`:

```promql
max(metrics_DirectMemoryUsageRatio_Value{...} and on (instance) metrics_IsDecommissioningWorker_Value{...} == 0)
```

Also `metricType: Value`. Keep the threshold below 0.85 so capacity arrives before pushes
pause; the 0.70 default leaves the gap between scaling out and throttling.

Both exclude decommissioning workers. A worker that is draining still holds its disk and its
slots for as long as it takes, and counting it would have the fleet scale out to replace
capacity it has not released yet.

Disable either with `diskUsage.enabled: false` / `memoryUsage.enabled: false`, and add your own
with `worker.autoscaling.triggers`, which are appended to the built-in ones and rendered
through `tpl` against the zone's context, so `{{ .zone.name }}` resolves per zone.

The built-in queries select on `role="Worker"`, the release namespace and the pod names of the
statefulset they scale, so several Celeborn clusters can share one Prometheus - or one
namespace - without scaling on each other's workers.

Celeborn exports gauges as `metrics_<Name>_Value` and counters as `metrics_<Name>_Count`. Other
metrics worth scaling on are `ActiveShuffleSize` and `ActiveShuffleFileCount` (data held),
`ActiveSlotsCount` (slots allocated), and `IsHighWorkload` / `PausePushDataStatus` (the worker
is already in trouble).

`worker.zoneAwareReplication.metricsLabel` passes the zone into `celeborn.metrics.extraLabels`
so the worker stamps it on everything it emits. The built-in queries do not use it - a `zone`
label says nothing about which release a worker belongs to - but it is there for queries of your
own. `role="Worker"` matters too: masters report the device gauges for whichever volume holds
the Ratis directory.

`minReplicaCount` defaults to the statefulset's own replica count, so a zone never scales
below the size it was deployed at unless you set it explicitly.

Enable `worker.drain` as well. Scaling in deletes worker pods, and without the drain those
workers leave with shuffle data an application may still need - see
[Draining a worker before it is removed](#draining-a-worker-before-it-is-removed).

Three properties of this fleet are worth sizing for. Scaling out is slow: a new worker
usually needs a new node, and it starts with no shuffle data on it, so autoscaling answers
sustained load changes rather than bursts. Scaling in always removes the highest ordinal,
which is not necessarily the least busy worker. And while a worker drains, its statefulset
cannot grow - so a scale-in decided at low load leaves that zone one worker short until the
drain finishes. Pace scale-in conservatively with `behavior.scaleDown`, and consider
excluding draining workers from the trigger query.

### Helm and the autoscaler both own `replicas`

The chart keeps rendering `spec.replicas`, so a fresh install starts at the size you asked
for rather than at one. The cost is that anything reconciling the rendered manifest will fight
KEDA over that field.

A plain `helm upgrade` is included in that: its three-way merge patches a KEDA-scaled replica
count back to the chart's value, and with `OrderedReady` the controller then removes the excess
ordinals one at a time, waiting for each to terminate - which is slow once `worker.drain` is
enabled, and slower again with `alwaysDecommission`. Keep `worker.replicas` in step with where
KEDA has settled, or pass `--set worker.replicas=<current>` when upgrading.

For a continuous-delivery tool, tell it to ignore the field - in Argo CD, `ignoreDifferences`
on `/spec/replicas` for the worker statefulsets, with `RespectIgnoreDifferences=true`.

## Documentation

For additional details on deploying the Celeborn Kubernetes Helm chart, please refer to the [Celeborn on Kubernetes](https://celeborn.apache.org/docs/latest/deploy_on_k8s/) documentation.
