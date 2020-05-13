# Schema Registry Helm Chart

Installs [Pravega Schema Registry](https://github.com/pravega/schema-registry) to create/configure/manage Pravega clusters atop Kubernetes.

## Introduction

This chart bootstraps a [schema registry](https://github.com/pravega/schema-registry) deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites
  - Kubernetes 1.15+
  - Helm 3+

## Installing the Chart

To install the chart with the release name `my-release`:

```
$ helm install my-release charts/schema-registry
```

This command deploys schema registry on the Kubernetes cluster with default configuration. The [configuration](#configuration) section lists the parameters that can be configured during installation.

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```
$ helm uninstall my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Configuration

The following table lists the configurable parameters of the schema registry chart and their default values.

| Parameter | Description | Default |
| ----- | ----------- | ------ |
| `replicaCount` | Number of replicas | 1 |
| `image.repository` | Repository for schema registry image | `pravega/schema-registry` |
| `image.tag` | Tag for schema registry image | `0.1.0` |
| `image.pullPolicy` | Pull policy for schema registry image | `IfNotPresent` |
| `service.type` | Schema registry service type | `LoadBalancer` |
| `service.port` | Schema registry service port | `9092` |
| `controller.ip` | IP address of the Pravega Controller service | `localhost` |
| `controller.port` | Port number of the Pravega Controller service | `9090` |
