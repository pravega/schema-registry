/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.serializationformats;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.samples.generated.Test1;
import io.pravega.schemaregistry.samples.generated.Test2;
import io.pravega.schemaregistry.samples.generated.Test3;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecordBase;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample class that demonstrates how to use Avro Serializers and Deserializers provided by Schema registry's 
 * {@link SerializerFactory}.
 * Avro has multiple deserialization options 
 * 1. Deserialize into Avro generated java class (schema on read).
 * 2. Deserialize into {@link GenericRecord} using user supplied schema (schema on read). 
 * 3. Deserialize into {@link GenericRecord} while retrieving writer schema. 
 * 4. Multiplexed Deserializer that deserializes data into one of avro generated typed java objects based on {@link SchemaInfo#type}.
 */
@Slf4j
public class AvroDemo {
    private static final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private static final Schema SCHEMA2 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("backwardPolicy compatible with schema1")
            .endRecord();

    private static final Schema SCHEMA3 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("c")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private final ClientConfig clientConfig;

    private final SchemaRegistryClient client;
    private final String id;

    public AvroDemo() {
        clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:9092")).build();
        client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        id = Long.toString(System.currentTimeMillis());
    }
    
    public static void main(String[] args) throws Exception {
        AvroDemo demo = new AvroDemo();
        demo.testAvroSchemaEvolution();

        demo.testAvroReflect();
        demo.testAvroGenerated();
        demo.testAvroMultiplexed();

        System.exit(0);
    }
    
    private void testAvroSchemaEvolution() {
        System.out.println("demoing avro schema evolution");
        // create stream
        String scope = "scope" + id;
        String stream = "avroevolution";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            System.out.println("adding new group with: \nserialization format = avro\n compatibiity = backwardPolicy");

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            System.out.println("registering schema " + SCHEMA1.toString(true));
            AvroSchema<GenericRecord> schema1 = AvroSchema.ofRecord(SCHEMA1);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();

            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            // region writer with schema1
            Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

            EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
            writer.writeEvent(record).join();
            // endregion

            System.out.println("registering schema with default value for new field:" + SCHEMA2.toString(true));

            AvroSchema<GenericRecord> schema2 = AvroSchema.ofRecord(SCHEMA2);

            // region writer with schema2
            serializer = SerializerFactory.avroSerializer(serializerConfig, schema2);

            writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
            writer.writeEvent(record).join();
            // endregion

            // region writer with schema3
            // this should throw exception as schema change is not backwardPolicy compatible.
            boolean exceptionThrown = false;
            try {
                System.out.println("registering schema " + SCHEMA3.toString(true));

                AvroSchema<GenericRecord> schema3 = AvroSchema.ofRecord(SCHEMA3);

                SerializerFactory.avroSerializer(serializerConfig, schema3);
            } catch (Exception ex) {
                exceptionThrown = true;
                System.out.println("schema registration failed with " + Exceptions.unwrap(ex));
            }
            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream + System.currentTimeMillis();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                System.out.println("reading all records into schema2" + SCHEMA2.toString(true));

                AvroSchema<Object> readSchema = AvroSchema.of(SCHEMA2);

                Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

                EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                // read two events successfully
                EventRead<Object> event = reader.readNextEvent(1000);
                assert event.getEvent() != null;
                event = reader.readNextEvent(1000);
                assert event.getEvent() != null;

                // create new reader, this time with incompatible schema3
                try (ReaderGroupManager readerGroupManager2 = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                    String rg1 = "rg1" + stream;
                    readerGroupManager2.createReaderGroup(rg1,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    System.out.println("creating new reader to read using schema 3:" + SCHEMA3.toString(true));

                    readSchema = AvroSchema.of(SCHEMA3);

                    exceptionThrown = false;
                    try {
                        SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);
                    } catch (Exception ex) {
                        exceptionThrown = Exceptions.unwrap(ex) instanceof IllegalArgumentException;
                        System.out.println("schema validation failed with " + Exceptions.unwrap(ex));
                    }
                    assert exceptionThrown;

                    // endregion
                    // region read into writer schema
                    String rg2 = "rg2" + stream;
                    readerGroupManager2.createReaderGroup(rg2,
                            ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                    deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

                    reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

                    event = reader.readNextEvent(1000);
                    System.out.println("event read =" + event.getEvent());
                    assert event.getEvent() != null;

                    event = reader.readNextEvent(1000);
                    System.out.println("event read =" + event.getEvent());
                    assert event.getEvent() != null;
                    // endregion
                }
            }
        }
    }
    
    private void testAvroReflect() {
        // create stream
        String scope = "scope" + id;
        String stream = "avroreflect";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<TestClass> schema = AvroSchema.of(TestClass.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();

            // region writer
            Serializer<TestClass> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<TestClass> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new TestClass("test")).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream + System.currentTimeMillis();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                AvroSchema<Object> readSchema = AvroSchema.of(ReflectData.get().getSchema(TestClass.class));

                Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

                EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<Object> event = reader.readNextEvent(1000);
                assert null != event.getEvent();

                // endregion
                // region read into writer schema
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

                reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

                event = reader.readNextEvent(1000);
                assert null != event.getEvent();
                // endregion
            }
        }
    }
    
    private void testAvroGenerated() {
        // create stream
        String scope = "scope" + id;
        String stream = "avrogenerated";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<Test1> schema = AvroSchema.of(Test1.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Serializer<Test1> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<Test1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new Test1("test", 0)).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream + System.currentTimeMillis();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                AvroSchema<Test1> readSchema = AvroSchema.of(Test1.class);

                Serializer<Test1> deserializer = SerializerFactory.avroDeserializer(serializerConfig, readSchema);

                EventStreamReader<Test1> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<Test1> event = reader.readNextEvent(1000);
                assert null != event.getEvent();

                // endregion
                // region read into writer schema
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<Object> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

                EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<Object> event2 = reader2.readNextEvent(1000);
                assert null != event2.getEvent();
                // endregion
            }
        }
    }
    
    private void testAvroMultiplexed() {
        // create stream
        String scope = "scope" + id;
        String stream = "avromultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        try (StreamManager streamManager = new StreamManagerImpl(clientConfig)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

            SerializationFormat serializationFormat = SerializationFormat.Avro;
            client.addGroup(groupId, new GroupProperties(serializationFormat,
                    Compatibility.backward(),
                    true));

            AvroSchema<SpecificRecordBase> schema1 = AvroSchema.ofSpecificRecord(Test1.class);
            AvroSchema<SpecificRecordBase> schema2 = AvroSchema.ofSpecificRecord(Test2.class);
            AvroSchema<SpecificRecordBase> schema3 = AvroSchema.ofSpecificRecord(Test3.class);

            SerializerConfig serializerConfig = SerializerConfig.builder()
                                                                .groupId(groupId)
                                                                .registerSchema(true)
                                                                .registryClient(client)
                                                                .build();
            // region writer
            Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
            map.put(Test1.class, schema1);
            map.put(Test2.class, schema2);
            map.put(Test3.class, schema3);
            Serializer<SpecificRecordBase> serializer = SerializerFactory.avroMultiTypeSerializer(serializerConfig, map);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            EventStreamWriter<SpecificRecordBase> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
            writer.writeEvent(new Test1("test", 0)).join();
            writer.writeEvent(new Test2("test", 0, "test")).join();
            writer.writeEvent(new Test3("test", 0, "test", "test")).join();

            // endregion

            // region read into specific schema
            try (ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig))) {
                String rg = "rg" + stream + System.currentTimeMillis();
                readerGroupManager.createReaderGroup(rg,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<SpecificRecordBase> deserializer = SerializerFactory.avroMultiTypeDeserializer(serializerConfig, map);

                EventStreamReader<SpecificRecordBase> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

                EventRead<SpecificRecordBase> event1 = reader.readNextEvent(1000);
                assert null != event1.getEvent();
                assert event1.getEvent() instanceof Test1;
                EventRead<SpecificRecordBase> event2 = reader.readNextEvent(1000);
                assert null != event2.getEvent();
                assert event2.getEvent() instanceof Test2;
                EventRead<SpecificRecordBase> event3 = reader.readNextEvent(1000);
                assert null != event3.getEvent();
                assert event3.getEvent() instanceof Test3;

                // endregion
                // region read into writer schema
                String rg2 = "rg2" + stream;
                readerGroupManager.createReaderGroup(rg2,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Serializer<Object> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

                EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

                EventRead<Object> genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                genEvent = reader2.readNextEvent(1000);
                assert null != genEvent.getEvent();
                // endregion

                // region read using multiplexed and generic record combination
                String rg3 = "rg3" + stream;
                readerGroupManager.createReaderGroup(rg3,
                        ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

                Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map2 = new HashMap<>();
                // add only two schemas
                map2.put(Test1.class, schema1);
                map2.put(Test2.class, schema2);

                Serializer<Either<SpecificRecordBase, Object>> eitherDeserializer =
                        SerializerFactory.avroTypedOrGenericDeserializer(serializerConfig, map2);

                EventStreamReader<Either<SpecificRecordBase, Object>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

                EventRead<Either<SpecificRecordBase, Object>> e1 = reader3.readNextEvent(1000);
                assert e1.getEvent() != null;
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof Test1;
                
                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isLeft();
                assert e1.getEvent().getLeft() instanceof Test2;
                
                e1 = reader3.readNextEvent(1000);
                assert e1.getEvent().isRight();
                //endregion
            }
        }
    }
    
    private static class TestClass {
        private final String test;

        private TestClass(String test) {
            this.test = test;
        }

        public String getTest() {
            return test;
        }
    }
}

