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

## Documentation

For additional details on deploying the Celeborn Kubernetes Helm chart, please refer to the [Celeborn on Kubernetes](https://celeborn.apache.org/docs/latest/deploy_on_k8s/) documentation.
