# How to Deploy RSS on Kubernetes

## Prerequisite
RSS is recommended to be deployed on nodes with local disk. Before starting, please make sure local disks on nodes are mounted to specific path.

## [Optional] Build RSS docker image
We have provided a docker image for RSS in helm chart. If you want to build your own RSS image with specific version, run docker build with our Dockerfile.

`
docker build -f docker/Dockerfile --build-arg rss_version=0.1.0 -t ${your-repo}:${tag} .
`

You can use `--build-arg rss_version` to indicates the version of RSS currently in use, default value is 0.1.1.

Make sure you have already built RSS, the target file 'rss-${project.version}-bin-release.tgz' is in ${RSS_HOME}.

## Deploy RSS with helm

#### Install kubectl and Helm

Please install and config kubectl and Helm first. See [Installing kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) and [Installing Helm](https://helm.sh/docs/intro/install/).

#### Create namespace on kubernetes
`
kubectl create ns rss
`

#### [Optional] Modify helm values file values.yaml
You can modify helm values file and set up customed deployment configuration.
`
vim ${RSS_HOME}/docker/helm/values.yaml
`
These values are suggested to be checked before deploy:  
- masterReplicas (number of rss master)
- workerReplicas (number of rss worker)
- rssVersion (rss release version)
- rss.worker.base.dirs (local disk mount path on k8s node)

For more information of RSS configurations, see [CONFIGURATIONS](../CONFIGURATION_GUIDE.md)

#### Install RSS
`
helm install rss-helm ${RSS_HOME}/docker/helm -n ${rss namespace}
`

#### Connect to RSS in K8s pod
After installation, you can connect to RSS master through headless service. For example, this is the spark configuration for 3-master RSS:
`
spark.rss.ha.master.hosts=shuffleservice-master-0.rss-svc.${rss namespace},shuffleservice-master-1.rss-svc.${rss namespace},shuffleservice-master-2.rss-svc.${rss namespace}
`

#### Uninstall RSS
`
helm uninstall rss-helm -n ${rss namespace}
`