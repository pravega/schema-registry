<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

## Schema Registry Terminology

### Schema
A schema defines the structure of your data in a formal IDL. Different serialization systems have different IDLs for describing the schema. Schema registry service stores these schema artifacts and allows their safe evolution and stores a versioned history of changes to the schema. 

### Schema Evolution
Schema represents the structure of your data defined for fulfilling the business needs. However, as business needs change, the structure of the data may need to be modified to reflect the changed reality. Changing of schema definition to incorporate newer requirements is called schema evolution. Schema evolution should be supported by the underlying serialization system that should allow different schemas to be used during writing and reading of data, subject to compatibility between these schemas. We will describe 
As schema is evolved, the newer structure is referred to as new version of the schema definition. Schema registry service allows users to safely evolve their schema subject to a chosen compatibility policy.  

### Schema Compatibility
Two schemas are said to be compatible if data written using one schema can be read using another schema. The compatibility can be in forward of backward directions. Backward compatibility refers to a situation where the consumers of data can use their schema to read data written with older version of the schema. Forward compatibility refers to situation where consumers can use older version of schema to read data written with newer version of schema. 

### Serialization Format
Serialization format refers to the serialization system used by the users to serialize and deserialize their data. For schema registry's purposes we are interested in schema based serialization systems, where the system includes an IDL which is used to describe the structure/schema. Schema registry service allows users to choose among different serialization systems and for storing and managing their schemas. Schema registry service has first class support for Avro, Protobuf and Json serialization formats where the service parses and validates these schemas to be well formed. However, the support from the service is not limited to these three formats and users can choose any other serialization system of their choice and store their schemas with the registry service too. If any other format is chosen, then schema registry service treats those schemas as an opaque blob and simply stores the schema without performing any validations or policy enforcement on it. 

### Namespace
A namespace is a named object which is simply an organizational construct which does not have any metadata. A namespace is optional and defaults to a `Null` name. Users of registry service can create their service entities with namespace tags of their choice and all resources created under the namespace are required to be unique within it. 
 
### Schema Group
A schema group is the chief resource in Schema Registry under which related schemas are added and evolved. A schema group is named and its name uniquely identifies it within a namespace. Schema registry service stores a versioned history of schemas under the schema group resource and also has user defined policies which are enforced as schemas are evolved.  

### Schema Type
Schema Type refers to the `name` of the object that the schema represents. Schema registry service applies compatibility policy on schema by comparing it with one or more older schemas that share the same `type`/`name` within a schema group. 

### Schema Version: 
As schema defining a specific structure is evolved and the new structure is registered with the service, it is assigned a monotonically increasing version number for that schema type. Schema registry service stores these versions as a chronological history of schemas. Each schema version in the service is uniquely identified by the tuple schema type, schema version and serialization format.  

### Schema Id
Schema registry service also assigns a unique 4 byte numerical identifier that uniquely identifies a schema in the schema group. A schema id is the id to uniquely identify the tuple - {schema type schema version and serialization format}. 

### Schema Group Properties
A Schema Group has an associated set of metadata configurations which users specify for serialization format and compatibility policy to be applied when evolving the schema.
Users have a choice to limit a schema group to only include schemas in a specific format (e.g. avro or protobuf) or choose `Any` which allows schemas from all serialization formats. Users also choose a compatibility policy that is applied on each new schema that is registered within the group.  

### Compatibility Policy
Evolution of schema is the need of the business. However, this has consequence on the behaviour of consumers of data. If schema is not evolved in accordance with the expectation of consumers of the data, then it could break their functionality. Schema registry service provides a host of compatibility policy choices to safely evolve your schema. Compatibility can be defined as schema group configuration. The policy is applied at the time when new schema is added to the group. Pravega schema registry supports a range of compatibility policies that can be specified:

    ALLOW_ANY: Allow all schema changes without any compatibility check.
    BACKWARD: Validate that new schema can be used to read data written using previous schema.
    BACKWARD_TILL(x): Validate that new schema is backward compatible till specified schema version.
    BACKWARD_TRANSITIVE: Validate that new schema is backward compatible with all previous schemas in the group.
    FORWARD: Validate that previous schema can be used to read data written using newer schema.
    FORWARD_TILL(x): Validate that new schema is backward compatible till specified schema version.
    FORWARD_TRANSITIVE: Validate that the new schema is forward compatible with all previous schemas in the group. 
	BACKWARD_TILL(x)_AND_FORWARD_TILL(y): Validate for backward compatibility till version identified by x and for forward compatibility till version identified by y.
    FULL: Validate against the latest schema that schema is compatible in both forward and backward direction with new schema. Old schema can be used to read data from new schema and new schema can be used to read data from old schema.
    FULL_TRANSITIVE: Validate against all previous schemas for both forward and backward compatibility.
    DENY_ALL: Disallow any schema evolution/modification.

### Codec Type
Apart from serializing the data, applications may want to apply additional encoding on the data like compression or encryption for instance. Schema registry service allows users to record the encoding information for the data as a pair of schema and a codec type. Codec type refers to the name of additional encoding applied on the data. Users can register zero or more codec types with the service where each codec type has a unique name and an optional collection of key value strings based properties. The codec type and schema version can be used by users to request schema registry service to generate a unique id identifying each such pair. This identifier is called encoding id. Pravega applications tag the event written into schema registry service with the encoding id. Pravega reader applications, upon seeing the encoding id, resolve it to schema and codec pair to decode and deserialize the data. 



