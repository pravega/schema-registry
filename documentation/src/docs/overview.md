<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Schema Registry Overview

Pravega Schema Registry is the latest service offering from Pravega family. The registry service is designed to store and manage schemas for the unstructured data stored in Pravega streams. The service is designed to not be limited to the data stored in Pravega and can serve as a general purpose management solution for storing and evolving schemas in wide variety of streaming and non streaming use cases.

It provides RESTful interface to store and manage schemas under schema groups. Schema groups are named groups under which schemas are registered and the service manages their versioned. Users can safely evolve their schemas within the context of the schema group based on desired schema compatibility policy configured at a group level. The service has built in support for popular serialization formats in Avro, Profobuf and JSON schemas, however, users can also store and manage schemas from any serialization system. The service allows users to specify desired compatibility policies for evolution of their schemas but these are employed only for the natively supported serialization systems.

Along with schemas, users can also use schema registry service to store additional encoding information about their data in pravega. The service exposes APIs to allow users to register additional codec information which includes the name of the encoding scheme used and any additional properties in form of key value pairs of strings. Encoding information would typically correspond to different compression or encryption used while encoding the serialized data at rest. The service generates unique identifiers for schemas and codec information pairs that users may use to tag their data with. 


Please refer to following wiki pages for installation and usage samples. 

## Releases

The latest Pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Running Schema Registry

Prerequesites: Pravega. Make sure you have pravega deployed and running. 

Schema registry can be run in standalone or in a distributed mode. The installation and deployment of Schema Registry is covered in the [Running Schema Registry](installation-guide.md) guide.

## Schema registry components
There are three main components involved in schema registry.
1.    Schema Registry Service – The service exposes a RESTful endpoint and uses pravega key value tables to durably store and evolve schemas.
2.    Java Client - This provides APIs to users to manage their schema groups and schemas. It uses a jersey rest client to talk to schema registry service.  
3.    Serializers - Schema registry aware Serializers that can be used in Pravega's EventStreamWriter and EventStreamReader. It comes with serializers for avro protobuf and json and support for providing custom serializer implementation too.  

## Schema Registry REST APIs
Schema registry REST API documentation can be found [here](rest-documentation.md). 
Sample curl commands for REST APIs can be found [here](rest-usage.md). 

## Using Schema Registry in Pravega Applications 

Examples of Pravega applications using schema registry serializers can be found [here](pravega-applications.md)

## Pravega Security, Role-based access control and TLS

Just like pravega, Schema Registry also supports encryption of all communication channels and pluggable role-based access control. 
Please refer [here](security.md) for more details on authentication and authorization.

## Support

Don’t hesitate to ask! Contact the developers and community on the [Slack](https://pravega-io.slack.com/) or email at security@pravega.io if you need any help.
Please open an issue in [Github Issues](https://github.com/pravega/pravega/issues) if you find a bug.

## Contributing

Become one of the **contributors!** We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
Please check the [Contributions Guidelines](contributing.md) to quickly understand on how to contribute to Pravega? You can see the [Roadmap](roadmap.md) document for more information.

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
