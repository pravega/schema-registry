# Pravega Schema Registry Repository

Pravega Schema Registry is the latest service offering from Pravega family. The registry service is designed to store and manage schemas for the unstructured data stored in Pravega streams. The service is designed to not be limited to the data stored in Pravega and can serve as a general purpose management solution for storing and evolving schemas in wide variety of streaming and non streaming use cases. 

It provides RESTful interface to store and manage schemas under schema groups. Users can safely evolve their schemas within the context of the schema group based on desired schema compatibility policy configured at a group level. The service has built in support for popular serialization formats in Avro, Profobuf and JSON schemas, however, users can also store and manage schemas from any serialization system. The service allows users to specify desired compatibility policies for evolution of their schemas but these are employed only for the natively supported serialization systems. 

Along with providing a storage layer for schema, the service also stores and manages additional encoding information in form of codec information. Codecs could correspond to different compression or encryption used while encoding the serialized data at rest. The service generates unique identifiers for schemas and codec information pairs that users may use to tag their data with. 

Please find relevant documentation and usage samples at following links:
- [Design Proposal](https://github.com/pravega/schema-registry/wiki/PDP-1:-Schema-Registry)
- [REST API documentation](https://github.com/pravega/schema-registry/wiki/REST-documentation)
- [Installation Guide](https://github.com/pravega/schema-registry/wiki/Installation-Guide)
- [REST API usage examples](https://github.com/pravega/schema-registry/wiki/REST-API-Usage-Samples)
- [Pravega Application usage examples](https://github.com/pravega/schema-registry/wiki/Sample-Usage:-Pravega-Application)

## Quick Start
----------
Schema Registry uses Pravega to store the schemas durably. Following steps assumes you have pravega deployed and running. 

### Running Schema Registry 
-----------------------------
To start schema registry locally. 
```
1. ./gradlew install
2. cd server/build/distributions/
3. uncompress schema-registry-<version>.tar or schema-registry-<version>.zip
4. cd schema-registry-<version>
5. change CONTROLLER_URL in conf/schema-registry.config.properties
6. ./bin/schema-registry
```
The above will start the schema registry server listening on port 9092. 

### Helm Chart
-----------------------------

Schema registry also includes Helm Charts to deploy Schema Registry service on a Kubernetes cluster.
Detailed instructions can be found [here](https://github.com/pravega/schema-registry/blob/master/charts/schema-registry/README.md)

```
helm install <release-name> charts/schema-registry
```
The charts can be configured to change the number of replicas, supply TLS configuration, controller uri and other schema regsitry configurations. 

## Development
-----------

Fork and clone schema registry repository. 

To build:

```bash
./gradlew build -x test
```

To run the unit and integration tests:

```bash
./gradlew test 
```

License
-------
The project is licensed under [Apache 2.0 license](LICENSE-Apache).


