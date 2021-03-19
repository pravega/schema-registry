<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Getting Started

Schema registry service allows you to manage and serve your schemas for the data in pravega streams. 
Best way to get started with schema registry is to see it in action in a pravega application. 
This document assumes you are familiar with and know how to use pravega SDK to write and read data into pravega.  

## Schema Registry Service

Before we deploy schema registry service, make sure you have pravega setup. Please refer to pravega's [getting-started](https://github.com/pravega/pravega/blob/master/documentation/src/docs/getting-started.md) to run pravega in standalone mode.

**Verify the following prerequisite**

```
Java 8 or higher
```

Once you have started pravega in standalone mode, it is time to run schema registry service. 

**Download Schema Registry**

Download the Schema registry release from the [Github Releases](https://github.com/pravega/schema-registry/releases).

You can also choose to clone and build schema registry service. 
```
git clone https://github.com/pravega/schema-registry.git
```
After you clone the schema registry repository run `./gradlew distribution` and `cd build/distribution`. 

Uncompress the distribution and you are ready to start the schema registry service. 
```
$ tar xfvz schema-registry-<version>.tgz
```

```
$ cd schema-registry-<version>
```
Note: the default configuration assumes schema registry service can access pravega running on tcp://localhost:9090. However, if you wish to point to a different pravega deployment, edit `conf/schema-registry.config.properties` and change pravega connection related configurations starting with pravega controller's url. 
```
schemaRegistry.store.pravega.controller.connect.uri=${CONTROLLER_URL}
```

**Running schema registry service without Pravega**
You can also run schema-registry without Pravega by changing the store type in configuration file `conf/schema-registry.config.properties` to `InMemory`.
```
schemaRegistry.store.type.name=InMemory
``` 
The above will run schema registry in a standalone mode where all schemas will only be stored in the process's memory and will be lost whenever the process is stopped. This configuration should only be used for testing and demo purposes and never used in production. 

Once you have configured schema registry, you can start the service by running following command
```
$ bin/schema-registry

```
This will bring up schema registry service which start a REST endpoint listening on port 9092. Now you are ready to manage and serve your schemas for your pravega streams. 

## Running a sample Pravega Application with schema registry

Schema registry integrates with pravega applications using the Serializer interface. Schema registry provides serializers modules which can be used in your application to use schemas registered with schema registry service.
Include following maven dependency in your application:
```
 <dependency>
        <groupId>io.pravega</groupId>
            <artifactId>schemaregistry-serializers</artifactId>
            <version>0.2.0</version>
 </dependency>
```

Then you use schema registry serializers by instantiating them in your application and using them with pravega writer and reader SDK.

```
    // 1. Create schema registry client configuration with the registry service url:
    SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
	        .schemaRegistryUri(registryUri).build();
    
	// 2. Create serializer config where you can optionally tell the SDK to automatically create a new schema group and register the schema before using it. 
    SerializerConfig serializerConfig = SerializerConfig.builder()
	        .groupId(groupId).registryConfig(config)
                .createGroup(SerializationFormat.Avro).registerSchema(true)                
                .build();
    
	// 3. Instantiate serializer with the Schema to use. 
    Serializer<User> serializer = SerializerFactory.avroSerializer(serializerConfig, AvroSchema.of(User.class));
    
	// 4. Use serializer with Pravega Writers. 
    EventStreamWriter<User> writer = clientFactory.createEventWriter(
	        stream, serializer, EventWriterConfig.builder().build());
    writer.writeEvent(new User("test", 1000)).join();

    // 5. Similarly create deserializer 
    Serializer<User> deserializer = SerializerFactory.avroDeserializer(
	                serializerConfig, AvroSchema.of(User.class));
	
	// 5.1 You can also create generic deserializer which deserializes your data into a generic object
	// (for example: GenericRecord for avro, DynamicMessage for Protobuf and JsonNode for json)
    Serializer<Object> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig);

	// 6. instantiate pravega reader and use schema registry deserializer. 
    readerGroupManager.createReaderGroup(rg, ReaderGroupConfig.builder()
	             .stream(NameUtils.getScopedStreamName(scope, stream)).build());
    EventStreamReader<User> reader = clientFactory.createReader(
	                "r1", rg, deserializer, ReaderConfig.builder().build());
    EventRead<User> event = reader.readNextEvent(1000);
```

We have developed a few samples to introduce the developer on how to use schema registry with pravega applications: [Pravega Schema Registry Samples](https://github.com/pravega/pravega-samples/tree/master/schema-registry-examples).

**Download the Pravega-Samples git repo**

```
$ git clone https://github.com/pravega/pravega-samples
$ cd pravega-samples/schema-registry-examples
```

**Generate the scripts to run the applications**

```
$ ../gradlew install
$ cd schema-registry-examples/build/install/pravega-schema-registry-examples
```

For instructions on how to run individual samples and look at the instructions [here](https://github.com/pravega/pravega-samples/blob/master/schema-registry-examples/README.md). 
