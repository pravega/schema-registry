# Installation Guide
<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

## Prepare your environment
Schema Registry uses Pravega to store the schemas durably. Following steps assumes you have pravega deployed and running. 

### Helm Chart
-----------------------------

Schema registry also includes Helm Charts to deploy Schema Registry service on a Kubernetes cluster.
Detailed instructions can be found [here](https://github.com/pravega/schema-registry/blob/master/charts/schema-registry/README.md)

```
helm install <release-name> charts/schema-registry
```
The charts can be configured to change the number of replicas, supply TLS configuration, controller uri and other schema regsitry configurations. 

### Running Schema Registry Service in standalone mode
-----------------------------

You can download released versions of schema registry from the [github release page](https://github.com/pravega/schema-registry/releases).

Alternatively, you can also clone the master and install and run the Schema Registry Service locally using following commands:
```
1. ./gradlew install
2. cd server/build/distributions/
```

It is prerequisite to run pravega, which the schema registry services uses as its durable storage. 
However, for testing/demo purposes it is also possible to run schema registry without a pravega deployment too, but it will not provide any durability and all schemas will be lost as soon as the service is stopped.
  
After downloading/installing the schema registry, 
```
3. uncompress schema-registry-<version>.tar or schema-registry-<version>.zip
4. cd schema-registry-<version>
5. change CONTROLLER_URL in conf/schema-registry.config.properties
6. ./bin/schema-registry
```
The above will start the schema registry server listening on port 9092. 

To run schema registry without pravega, in the conf/schema-registry.config.properties change the STORE_TYPE to `InMemory`. 

