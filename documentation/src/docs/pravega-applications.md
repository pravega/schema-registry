<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

### Usage in pravega applications
Schema registry repository comes with schema registry serializers that can be used in pravega applications when instantiating Pravega Event Writers or Readers. Schema Registry comes with serializers for different serialization formats like avro protobuf and json.
 
These serializers hide away all the interaction and stamping of data with schema registry generated encoding identifiers.
All the interaction with schema registry service is transparent to the user. They merely need to provide the schema registry URL and instantiate a serializer.

### Maven dependency
Schema registry is available on maven central repository. In your application's pom file add a dependency on one of schema registry serializers. It comes in four flavours - avro, protobuf, json and a serializers artifact that has all of the above and support for custom formats. 
The artifacts are available in both fat and lean jar options. For fat jars add the classifier `all`. 
```
all:
 <dependency>
        <groupId>io.pravega</groupId>
            <artifactId>schemaregistry-serializers</artifactId>
            <version>0.5.0</version>
 </dependency>

json only:
 <dependency>
        <groupId>io.pravega</groupId>
            <artifactId>schemaregistry-serializers-json</artifactId>
            <version>0.5.0</version>
 </dependency>

avro only:
 <dependency>
        <groupId>io.pravega</groupId>
            <artifactId>schemaregistry-serializers-avro</artifactId>
            <version>0.5.0</version>
 </dependency>

protobuf only:
 <dependency>
        <groupId>io.pravega</groupId>
            <artifactId>schemaregistry-serializers-protobuf</artifactId>
            <version>0.5.0</version>
 </dependency>
```

### Sample 
For trying out schema registry in pravega application, you may check out schema registry samples available with [pravega samples](https://github.com/pravega/pravega-samples). 

The following example demonstrates how schema registry avro serializer can be used with pravega clients to read and write avro data into pravega stream. 
Here, `User` is an avro generated java class. This is the object that will be written into and read from pravega streams. 

SchemaRegistry's SerializerFactory provides avro serializers and deserializers that are instantiated with AvroSchema for User class. 
The following is an example of detailed configuration that tells the SerializerFactory to automatically create the schema registry `Group` and automatically register the supplied schema before using it. The defaults for these are to not create the group and not add schema automatically. 

```

    
    // create serializer and deserializer
    SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
	        .schemaRegistryUri(registryUri).build();
    
    SerializerConfig serializerConfig = SerializerConfig.builder()
	        .groupId(groupId).registryConfig(config)
                .createGroup(SerializationFormat.Avro).registerSchema(true)                
                .build();
    
    Serializer<User> serializer = SerializerFactory
	        .avroSerializer(serializerConfig, AvroSchema.of(User.class));
    // writer 
    EventStreamWriter<User> writer = clientFactory.createEventWriter(
	        stream, serializer, EventWriterConfig.builder().build());
    writer.writeEvent(new User("test", 1000)).join();

    // reader
    readerGroupManager.createReaderGroup(rg, ReaderGroupConfig.builder()
	             .stream(NameUtils.getScopedStreamName(scope, stream)).build());

    // to use the writer schema, replace `AvroSchema.of(User.class)` with `null`
    Serializer<User> deserializer = SerializerFactory.avroDeserializer(
	                serializerConfig, AvroSchema.of(User.class));

    EventStreamReader<User> reader = clientFactory.createReader(
	                "r1", rg, deserializer, ReaderConfig.builder().build());
    EventRead<User> event = reader.readNextEvent(1000);
```

### Programmatic usage of Schema Registry 
We will focus on the programatic usage of schema registry in this document. REST api documentation is available [here](https://github.com/pravega/schema-registry/wiki/REST-documentation).

### Schema registry client 
Instantiate schema registry client:
```
    SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
	                                   .schemaRegistryUri(registryUri).build();
    SchemaRegistryClient client = SchemaRegistryClientFactory.createRegistryClient(config);
```

### Serializers 
Data path apis can also be invoked using REST clients but are typically meaningful while using schema registry supplied serializers. The invocation of data path apis is transparent to the user using these serializers. 

To instantiate serializers, users will supply a serializer configuration and the schema to use. 
Example serializer config:
```        
    // with registry client config 
    SerializerConfig serializerConfig = SerializerConfig.builder()
                                    .groupId(groupId)
                                    .registryConfig(config)
                                    .build();

    // with registry client
    SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                    .groupId(groupId)
                                    .registryClient(client)
                                    .build();
```

Instantiating a serializer is as simple as calling the appropriate factory method with the config and schema. 
example serializer:
```
    // avro
    Serializer<User> serializer = SerializerFactory.avroSerializer(
	            serializerConfig, AvroSchema.of(User.class));

    // protobuf 
    ProtobufSchema<Protobuf.User> schema = ProtobufSchema.of(
	                Protobuf.User.class);
    Serializer<Protobuf.User> serializer = SerializerFactory.protobufSerializer(
	                serializerConfig, schema);

    // json
    Serializer<MyUser> serializer = SerializerFactory.jsonSerializer(
	                serializerConfig, JSONSchema.of(MyUser.class));
```


### Add group 
As first step, the user needs to create schema group. Then it can add schemas and codecs to the group. Or use data path apis by instantiating Serializers to encode and decode user objects. 
Create Group can be created in two ways programmatically:
-    Instantiate Registry Client and call addGroup api on it
```
        client.addGroup(groupId, serializationFormat, schemaValidationRules, 
		                true, Collections.emptyMap());
```
-    Set `createGroup` in SerializerConfig
```
   SerializerConfig serializerConfig = SerializerConfig.builder()
                        .groupId(groupId)
                        .createGroup(SerializationFormat.Avro)
                        .registryConfig(config)
                        .build();

   SerializerConfig serializerConfig = SerializerConfig.builder()
                        .groupId(groupId)
                        .createGroup(SerializationFormat.Avro, Compatibility.backward(), true)
                        .registryConfig(config)
                        .build();

```

### Add Schema
Similar to add group, addSchema can also be performed in two ways programmatically:
-    Call addSchema api on schema registry client
```
        SchemaInfo schemaInfo = new SchemaInfo("User", SerializationFormat.Avro, 
                schema.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        VersionInfo version = client.addSchema(group, schemaInfo);

```
-    Set registerSchema in serializer config and use it with a schema while instantiating a serializer. The schema will be auto registered 
by the serializer factory before instantiating the serializer.  
```
        SerializerConfig serializerConfig = SerializerConfig.builder()
                    .groupId(groupId)
                    .registerSchema(true)
                    .registryConfig(config)
                    .build();
```

### Schema container objects
The serializer factory accepts schemas encapsulated under an implemetation of schema container interface. We provide implementation for `AvroSchema`, `ProtobufSchema` and `JSONSchema` with the serializers. For custom serialization format, users need to wrap their schema under a SchemaContainer object that is capable of converting the schema object to `SchemaInfo` object as defined by schema registry client. 

All three of AvroSchema, ProtobufSchema and JSONSchema can be used for both typed and generic schema objects.
```
    // avro
    AvroSchema<User> schema = AvroSchema.of(User.class);
    AvroSchema<Object> schema = AvroSchema.of(avroSchemaObj);

    // protobuf 
    ProtobufSchema<Protobuf.Message1> schema = ProtobufSchema.of(
	            Protobuf.Message1.class);
    ProtobufSchema<DynamicMessage> schema = ProtobufSchema.of(
	              "Protobuf.Message1", descriptorSet);

    // json
    JSONSchema<MyUser> schema = JSONSchema.of(MyUser.class);
    JSONSchema<Object> serializer = JSONSchema.of("MyUser", jsonSchemaString);
```


### Serializer Factory
Serializer factory class is used to create different types of serializers. 
For each of protobuf, avro and json, serializer factory provides serializer and deserializer implementations that can be used in pravega clients.
These serializers will transparently communicate with schema registry, and serialize/deserialize java objects. 
There is support for both typed java objects and serialization system specific generic record deserialization. 

Example of avro serializers and deserializers (similar set of serializers and deserializers available for protobuf and json)
```
    /**
     * Creates a typed avro serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     * 
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     */
    public static <T> Serializer<T> avroSerializer(SerializerConfig config, AvroSchema<T> schemaData);

    /**
     * Creates a typed avro deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     */
    public static <T extends IndexedRecord> Serializer<T> avroDeserializer(SerializerConfig config,
                                                                           AvroSchema<T> schemaData);

    /**
     * Creates a generic avro deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link Object}. All avro record types
     * are deserialized as {@link GenericRecord}.
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     */
    public static Serializer<Object> avroGenericDeserializer(
                                     SerializerConfig config, @Nullable AvroSchema<Object> schemaData);

```
Example writer:
```
    SerializerConfig serializerConfig = SerializerConfig.builder()
             .groupId(groupId)
             .registryConfig(config)
             .build();

    AvroSchema<User> schema = AvroSchema.of(User.class);
    Serializer<User> serializer = SerializerFactory.avroSerializer(
	          serializerConfig, schema);
```
Example reader:
```
    // Deserialize into typed java object
    Serializer<User> deserializer = SerializerFactory.avroDeserializer(
	          serializerConfig, schema);
        
    // Use writer schema to deserialize into generic record
    Serializer<Object> genericDeserializer = SerializerFactory.
	          avroGenericDeserializer(serializerConfig, null);
```

#### Multiple Event Types within same pravega stream
To support scenarios where multiple types of objects could be written into same pravega stream, SerializerFactory provides `multiType serializers and deserializers`. Similar serializers and deserializers are also available for Protobuf and json. 
This is typically beneficial for event sourcing and message bus or microservice communication scenarios. 
``` 
    /**
     * A multiplexed Avro serializer that takes a map of schemas and validates them individually.
     */
    public static <T extends IndexedRecord> Serializer<T> avroMultiTypeSerializer(SerializerConfig config,
                                                                                  Map<Class<? extends T>, AvroSchema<T>> schemas);

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     */
    public static <T extends SpecificRecordBase> Serializer<T> avroMultiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas);

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     */
    public static <T extends SpecificRecordBase> Serializer<Either<T, Object>> avroTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas);
```

Example writer and reader for multi type:
In following code example, `User` and `Address` are avro generated java classes. 
```
    SerializerConfig serializerConfig = SerializerConfig.builder()
                                            .groupId(groupId)
                                            .createGroup(serializationFormat,
                                                Compatibility.backward(),
                                                true)
                                            .registerSchema(true)
                                            .registryClient(client)
                                            .build();

    // add schemas into a map or type to schema.
    Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
    map.put(User.class, AvroSchema.ofSpecificRecord(User.class));
    map.put(Address.class, AvroSchema.ofSpecificRecord(Address.class));
        
    Serializer<SpecificRecordBase> serializer = SerializerFactory.avroMultiTypeSerializer(
	            serializerConfig, map);
    EventStreamWriter<SpecificRecordBase> writer = clientFactory.createEventWriter(
	            stream, serializer, 
				EventWriterConfig.builder().build());
    writer.writeEvent(new User("username"));
    writer.writeEvent(new Address("address", zipCode));
```

```
    Serializer<SpecificRecordBase> deserializer = SerializerFactory
	            .avroMultiTypeDeserializer(serializerConfig, map);
    EventStreamReader<SpecificRecordBase> reader = clientFactory
	            .createReader("r1", rg, deserializer, 
				ReaderConfig.builder().build());

    EventRead<SpecificRecordBase> event1 = reader.readNextEvent(10000);
    assertTrue(event1.getEvent() instanceof User);
    EventRead<SpecificRecordBase> event2 = reader.readNextEvent(10000);
    assertTrue(event2.getEvent() instanceof Address);
```

### Custom serializers 
Users can also supply their custom serializers and deserializers which the serializer factory will use for serializing and deserializing the payload while taking care of all interactions with schema registry service away from the serializer. 
```
    /**
     * A serializer that uses user supplied implementation of {@link PravegaSerializer} for serializing the objects.
     * It also takes user supplied schema and registers/validates it against the registry.
     */
    public static <T> Serializer<T> customSerializer(SerializerConfig config, SchemaContainer<T> schema, PravegaSerializer<T> serializer);

    /**
     * A deserializer that uses user supplied implementation of {@link PravegaDeserializer} for deserializing the data into
     * typed java objects.
     */
    public static <T> Serializer<T> customDeserializer(SerializerConfig config, @Nullable SchemaContainer<T> schema,
                                                       PravegaDeserializer<T> deserializer);
```

The user supplied serializer and deserializer can be used with above factory methods for custom serialization formats. The user will supply their custom schema bytes wrapped under the schemaInfo object. The registry will register the schema for the group without attempting to parse the schema. 
Note: for custom schema formats and custom serializers registry merely serves the schemas to the deserializers. 
Example:
```
    SchemaInfo schemaInfo = new SchemaInfo("User", serializationFormat, myPojoSchemaBytes, Collections.emptyMap());
    MySerializer mySerializer = new MySerializer();

    Serializer<MyPojo> serializer = SerializerFactory.customSerializer(
	            config, () -> schemaInfo, mySerializer);
    EventStreamWriter<MyPojo> writer = clientFactory.createEventWriter(
	            stream, serializer, EventWriterConfig.builder().build());

    MyDeserializer<MyPojo> myDeserializer = new MyDeserializer();
    Serializer<MyPojo> deserializer = SerializerFactory.customDeserializer(
	            serializerConfig, () -> null, myDeserializer);
    EventStreamReader<MyPojo> reader = clientFactory.createReader(
	            "r1", rg, deserializer, ReaderConfig.builder().build());
```

### Multiple format support within same pravega stream
There are some additional deserializers that are provided by SerializerFactory. These include multiFormatDeserializer, which can read the encoding information in the payload and based on the serialization format (one of avro protobuf and json) it deserializes the event into the serialization system specific generic java object. 
There is another flavour which converts the deserialized object into a json string, whereby allowing reader applications to work with jsons while writers could write data into any of avro, protobuf or json. 

```
    /**
     * A deserializer that can read data where each event could be written with either of avro, protobuf or json 
     * serialization formats.
     * An event serialized with avro is deserialized into {@link Object}. If its record type, it is {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link java.util.LinkedHashMap}.
     */
    public static Serializer<Object> genericDeserializer(SerializerConfig config);

    /**
     * This is a convenience serializer shortcut that calls {@link #deserializeAsT} with a transform to 
     * convert the object to JSON string.
     */
    public static Serializer<String> deserializeAsJsonString(SerializerConfig config);

    /**
     * A deserializer that can read data where each event could be written with different serialization formats. 
     * Formats supported are protobuf, avro and json. 
     * An event serialized with avro is deserialized into {@link Object}. If its record type, it is {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link java.util.LinkedHashMap}.
     *
     * This also takes a transform function which is applied on the deserialized object and should transform the object 
     * into the type T.  
     */
    public static <T> Serializer<T> deserializeAsT(SerializerConfig config,
                                                   BiFunction<SerializationFormat, Object, T> transform);
```

Example: Stream could have data written using avro, protobuf or json. The deserializer hides the details and gives the deserialized
object back. 
```
        // 1. read as a deserialized object 
        Serializer<Object> deserializer = SerializerFactory.genericDeserializer(serializerConfig);
        EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        EventRead<Object> event = reader.readNextEvent(1000);

        // 2. read as a json string
        Serializer<String> deserializer = SerializerFactory.deserializeAsJsonString(serializerConfig);
        EventStreamReader<String> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(1000);

        // 3. deserialize and transform it to an object of type T. 
        Serializer<T> deserializer = SerializerFactory.deserializeAsT(serializerConfig, (x, y) -> myTransformFunction(x, y));
        EventStreamReader<T> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        EventRead<T> event = reader.readNextEvent(1000);
```

#### Multi format automatically consume and produce data
There are two special serializers and deserializers meant to load data from one stream into another. These are namely `serializerWithSchema` and `deserializerWithSchema`. Deserializer with schema returns the object with the schema as it reads from a stream. This same data can be passed to the SerializerWithSchema and it would serialize the data with the given schema and the original format of the data. 

```
    /**
     * A multi format serializer that takes objects with schemas for the three supported formats - avro, protobuf and json.
     * It then serializes the object using the format specific serializer. The events are supplied using an encapsulating 
     * object called WithSchema which has both the event and the schema. 
     * It only serializes the events while ensuring that the corresponding schema was registered with the service. 
     * If {@link SerializerConfig#registerSchema} is set to true, it registers the schema before using it. 
     * This serializer contacts schema registry once for every new schema that it encounters, and it fetches the 
     * encoding id for the schema and codec pair. 
     */
    public static Serializer<WithSchema<Object>> serializerWithSchema(SerializerConfig config);

    /**
     * A deserializer that can deserialize data where each event could be written with either of avro, protobuf or json 
     * serialization formats. It deserializes them into format specific generic objects. 
     * An event serialized with avro is deserialized into {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link java.util.LinkedHashMap}.
     */
    public static Serializer<WithSchema<Object>> deserializerWithSchema(SerializerConfig config);
```

Example:
```
        // read from input stream
        Serializer<WithSchema<Object>> deserializer = SerializerFactory.deserializerWithSchema(serializerConfig);
        EventStreamReader<WithSchema<Object>> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        EventRead<WithSchema<Object>> event = reader.readNextEvent(1000);
        Object = event.getEvent().getObject();
        if (event.getEvent().hasAvroSchema()) {
             Schema avroSchema = event.getEvent().getAvroSchema();
        } 

// process the event, apply businesslogic. 
....

        // writing the event into destination stream
        Serializer<WithSchema<Object>> genericSerializer = SerializerFactory.serializerWithSchema(serializerConfig);
        EventStreamWriter<WithSchema<Object>> genericWriter = clientFactory
                .createEventWriter(outstream, genericSerializer, EventWriterConfig.builder().build());
        genericWriter.writeEvent(event.getEvent());
```

### Codecs
Schema registry serializers also support codecs. CodecType is supplied with the config while instantiating a serializer. 
Serializer library provides implementation for snappy and gzip codecs and has support for custom user defined codec types too. For serializers, users can supply the codec type. By default three decoders are included with each deserializer - decoders for `snappy`, `gzip` and `none` codec types. If no codec is supplied for serializer, CodecType.none is used. 
```
    SerializerConfig serializerConfig = SerializerConfig.builder()
         .groupId(groupId)
         .codec(CodecFactory.snappy())
         .registryClient(client)
         .build();
```

Custom codec type support is also supported by registry. 
Users can create their custom codecs and use custom codecType and register the codec type with the registry service. 
The encoding-decoding libraries for custom codec type needs to be available with writer and reader applications. 
```
    Codec myCodec = new Codec() {
        @Override
        public CodecType getCodecType() {
            return CodecType.custom("MyCustomCodecType", Collections.emptyMap());
        }

        @Override
        public ByteBuffer encode(ByteBuffer data) {
            // custom encoding logic
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) {
            // custom decoding logic
        }
    };
```
Usage with writers and readers:
``` 
    SerializerConfig serializerConfig = SerializerConfig.builder()
             .groupId(groupId)
             .codec(myCodec)
             .registerCodec(true)
             .registryClient(client)
             .build();

    SerializerConfig deserializerConfig = SerializerConfig.builder()
             .groupId(groupId)
             .addDecoder(CodecFactory.GZIP, CodecFactory.gzip::decode)
             .addDecoder(CodecFactory.SNAPPY, CodecFactory.snappy::decode)
             .addDecoder(myCodec.getCodecType(), myCodec::decode)
             .registryClient(client)
             .build();
```
