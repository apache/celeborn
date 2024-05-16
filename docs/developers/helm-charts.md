---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Helm Charts

## Run Helm Unit Tests

To run unit tests against Helm charts, first you need to install the helm unittest plugin as follows:

```shell
helm plugin install https://github.com/helm-unittest/helm-unittest.git --version=0.5.1
```

For detailed information about how to write helm chart unit tests, please refer [helm-unittest/helm-unittest](https://github.com/helm-unittest/helm-unittest). When you want to modify the chart templates or values, remember to update the related unit tests as well, otherwise the github CI may fail.

Unit tests are placed under `charts/celeborn/tests` directory, and can be running using the following command:

```shell
$ helm unittest charts/celeborn  --file "tests/**/*_test.yaml" --strict --debug

### Chart [ celeborn ] charts/celeborn

 PASS  Test Celeborn configmap  charts/celeborn/tests/configmap_test.yaml
 PASS  Test Celeborn master pod monitor charts/celeborn/tests/master/podmonitor_test.yaml
 PASS  Test Celeborn master priority class      charts/celeborn/tests/master/priorityclass_test.yaml
 PASS  Test Celeborn master service     charts/celeborn/tests/master/service_test.yaml
 PASS  Test Celeborn master statefulset charts/celeborn/tests/master/statefulset_test.yaml
 PASS  Test Celeborn worker pod monitor charts/celeborn/tests/worker/podmonitor_test.yaml
 PASS  Test Celeborn worker priority class      charts/celeborn/tests/worker/priorityclass_test.yaml
 PASS  Test Celeborn worker service     charts/celeborn/tests/worker/service_test.yaml
 PASS  Test Celeborn worker statefulset charts/celeborn/tests/worker/statefulset_test.yaml

Charts:      1 passed, 1 total
Test Suites: 9 passed, 9 total
Tests:       46 passed, 46 total
Snapshot:    0 passed, 0 total
Time:        177.518ms
```
