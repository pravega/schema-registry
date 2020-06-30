/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
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
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.samples.demo.objects.Address;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser1;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser2;
import io.pravega.schemaregistry.samples.demo.objects.User;
import io.pravega.schemaregistry.samples.generated.ProtobufTest;
import io.pravega.schemaregistry.samples.generated.Test1;
import io.pravega.schemaregistry.samples.generated.Test2;
import io.pravega.schemaregistry.samples.generated.Test3;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.JsonGenericObject;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Slf4j
public class TestPravegaClientEndToEnd implements AutoCloseable {
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

    @Rule
    public Timeout globalTimeout = new Timeout(3, TimeUnit.MINUTES);

    private final ClientConfig clientConfig;

    private final SchemaStore schemaStore;
    private final ScheduledExecutorService executor;
    private final SchemaRegistryService service;
    private final SchemaRegistryClient client;
    private final PravegaStandaloneUtils pravegaStandaloneUtils;
    private Random random;
    private final int port;
    private final RestServer restServer;

    public TestPravegaClientEndToEnd() throws Exception {
        pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        executor = Executors.newScheduledThreadPool(10);

        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaStandaloneUtils.getControllerURI())).build();

        schemaStore = SchemaStoreFactory.createPravegaStore(Config.SERVICE_CONFIG, clientConfig, executor);

        service = new SchemaRegistryService(schemaStore, executor);
        port = TestUtils.getAvailableListenPort();
        ServiceConfig serviceConfig = ServiceConfig.builder().port(port).build();

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();
        client =  SchemaRegistryClientFactory.createRegistryClient(
                SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:" + port)).build());
        random = new Random();
    }
    
    @Override
    @After
    public void close() throws Exception {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
    }
    
    @Test
    public void testAvroSchemaEvolution() {
        // create stream
        String scope = "scope";
        String stream = "avroevolution";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Avro;

        AvroSchema<Object> schema1 = AvroSchema.of(SCHEMA1);
        AvroSchema<Object> schema2 = AvroSchema.of(SCHEMA2);
        AvroSchema<Object> schema3 = AvroSchema.of(SCHEMA3);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.backward(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<Object> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<Object> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
        writer.writeEvent(record).join();
        // endregion
        
        // region writer with schema2
        serializer = SerializerFactory.avroSerializer(serializerConfig, schema2);

        writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
        writer.writeEvent(record).join();
        // endregion
        
        // region writer with schema3
        // this should throw exception as schema change is not backwardPolicy compatible.
        AssertExtensions.assertThrows("", () -> SerializerFactory.avroSerializer(serializerConfig, schema3),
                ex -> Exceptions.unwrap(ex) instanceof RegistryExceptions.SchemaValidationFailedException);
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<GenericRecord> readSchema = AvroSchema.ofRecord(SCHEMA2);

        Serializer<GenericRecord> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        // read two events successfully
        EventRead<GenericRecord> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());

        reader.close();
        // create new reader, this time with incompatible schema3
        
        String rg1 = "rg1" + stream;
        readerGroupManager.createReaderGroup(rg1, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        AvroSchema<GenericRecord> readSchemaEx = AvroSchema.ofRecord(SCHEMA3);
        
        AssertExtensions.assertThrows("",  () -> SerializerFactory.avroGenericDeserializer(serializerConfig, readSchemaEx), 
                ex -> Exceptions.unwrap(ex) instanceof IllegalArgumentException);
        reader.close();
        // endregion
        
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());

        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        reader.close();
        readerGroupManager.close();
        // endregion
    }

    @Test
    public void testCodec() {
        // create stream
        String scope = "scope";
        String stream = "avrocodec";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Avro;

        AvroSchema<Object> schema1 = AvroSchema.of(SCHEMA1);
        AvroSchema<Object> schema2 = AvroSchema.of(SCHEMA2);
        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.backward(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registerCodec(true)
                                                            .registryClient(client)
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<Object> serializer = SerializerFactory.avroSerializer(serializerConfig, schema1);
        
        EventStreamWriter<Object> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
        writer.writeEvent(record).join();
        // endregion

        // region writer with schema2
        Serializer<Object> serializer2 = SerializerFactory.avroSerializer(serializerConfig, schema2);

        writer = clientFactory.createEventWriter(stream, serializer2, EventWriterConfig.builder().build());
        record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
        writer.writeEvent(record).join();
        // endregion

        // region writer with codec gzip 
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registerSchema(true)
                                           .registerCodec(true)
                                           .codec(CodecFactory.gzip())
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer3 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer3 = clientFactory.createEventWriter(stream, serializer3, EventWriterConfig.builder().build());
        String bigString = generateBigString(100);
        writer3.writeEvent(new Test1(bigString, 1)).join();

        List<String> list = client.getCodecTypes(groupId);
        assertEquals(2, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.NONE)));
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.MIME_GZIP)));
        // endregion
        
        // region writer with codec snappy
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registerSchema(true)
                                           .registerCodec(true)
                                           .codec(CodecFactory.snappy())
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer4 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer4 = clientFactory.createEventWriter(stream, serializer4, EventWriterConfig.builder().build());
        String bigString2 = generateBigString(200);
        writer4.writeEvent(new Test1(bigString2, 1)).join();

        list = client.getCodecTypes(groupId);
        assertEquals(3, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.NONE)));
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.MIME_GZIP)));
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.MIME_SNAPPY)));
        // endregion
        
        // region reader
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registryClient(client)
                                           .build();
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> event = reader.readNextEvent(10000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            GenericRecord e = event.getEvent();
            event = reader.readNextEvent(10000);
        }
        // endregion

        // region writer with custom codec
        String mycodec = "mycodec";
        Codec myCodec = new Codec() {
            @Override
            public String getCodecType() {
                return mycodec;
            }

            @Override
            public ByteBuffer encode(ByteBuffer data) {
                return data;
            }

            @Override
            public ByteBuffer decode(ByteBuffer data) {
                return data;
            }
        };
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registerSchema(true)
                                           .registerCodec(true)
                                           .codec(myCodec)
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer5 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer2 = clientFactory.createEventWriter(stream, serializer5, EventWriterConfig.builder().build());
        String bigString3 = generateBigString(300);
        writer2.writeEvent(new Test1(bigString3, 1)).join();
        // endregion 

        list = client.getCodecTypes(groupId);
        assertEquals(4, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.NONE)));
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.MIME_GZIP)));
        assertTrue(list.stream().anyMatch(x -> x.equals(CodecFactory.MIME_SNAPPY)));
        assertTrue(list.stream().anyMatch(x -> x.equals(mycodec)));
        reader.close();

        // region new reader with additional codec
        // add new decoder for custom serialization
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .addDecoder(myCodec.getCodecType(), myCodec::decode)
                                                             .registryClient(client)
                                                             .build();

        Serializer<GenericRecord> deserializer2 = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        EventStreamReader<GenericRecord> reader2 = clientFactory.createReader("r2", rg, deserializer2, ReaderConfig.builder().build());

        event = reader2.readNextEvent(10000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            GenericRecord e = event.getEvent();
            event = reader2.readNextEvent(10000);
        }
        // endregion

        writer.close();        
        writer2.close();        
        writer3.close();        
        writer4.close();        
        reader.close();
        reader2.close();
        readerGroupManager.close();
    }

    private String generateBigString(int sizeInKb) {
        byte[] array = new byte[1024 * sizeInKb];
        random.nextBytes(array);
        return Base64.getEncoder().encodeToString(array);
    }

    @Test
    public void testAvroReflect() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avroreflect";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Avro;

        AvroSchema<TestClass> schema = AvroSchema.of(TestClass.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.backward(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        
        // region writer
        Serializer<TestClass> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<TestClass> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new TestClass("test")).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rgx" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<TestClass> readSchema1 = AvroSchema.of(TestClass.class);

        Serializer<TestClass> deserializer1 = SerializerFactory.avroDeserializer(serializerConfig, readSchema1);

        EventStreamReader<TestClass> reader1 = clientFactory.createReader("r1", rg, deserializer1, ReaderConfig.builder().build());

        EventRead<TestClass> event1 = reader1.readNextEvent(10000);
        assertNotNull(event1.getEvent());
        reader1.close();
        // endregion
        // region read into specific schema
        readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        AvroSchema<GenericRecord> readSchema = AvroSchema.ofRecord(ReflectData.get().getSchema(TestClass.class));

        Serializer<GenericRecord> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        reader.close();
        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        reader.close();
        readerGroupManager.close();
        // endregion
    }

    @Test
    public void testAvroGenerated() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avrogenerated";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Avro;

        AvroSchema<Test1> schema = AvroSchema.of(Test1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.backward(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Serializer<Test1> serializer = SerializerFactory.avroSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<Test1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new Test1("test", 1000)).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<Test1> readSchema = AvroSchema.of(Test1.class);

        Serializer<Test1> deserializer = SerializerFactory.avroDeserializer(serializerConfig, readSchema);

        EventStreamReader<Test1> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<Test1> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertEquals("test", event.getEvent().getName().toString());
        assertEquals(1000, event.getEvent().getField1());
        reader.close();
        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<GenericRecord> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        readerGroupManager.close();
        // endregion
    }

    @Test
    public void testAvroMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avromultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Avro;

        AvroSchema<SpecificRecordBase> schema1 = AvroSchema.ofSpecificRecord(Test1.class);
        AvroSchema<SpecificRecordBase> schema2 = AvroSchema.ofSpecificRecord(Test2.class);
        AvroSchema<SpecificRecordBase> schema3 = AvroSchema.ofSpecificRecord(Test3.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.backward(),
                                                                    true)
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
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<SpecificRecordBase> deserializer = SerializerFactory.avroMultiTypeDeserializer(serializerConfig, map);

        EventStreamReader<SpecificRecordBase> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<SpecificRecordBase> event1 = reader.readNextEvent(10000);
        assertNotNull(event1.getEvent());
        assertTrue(event1.getEvent() instanceof Test1);
        EventRead<SpecificRecordBase> event2 = reader.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        assertTrue(event2.getEvent() instanceof Test2);
        EventRead<SpecificRecordBase> event3 = reader.readNextEvent(10000);
        assertNotNull(event3.getEvent());
        assertTrue(event3.getEvent() instanceof Test3);

        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<GenericRecord> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        // endregion
        
        // region read using multiplexed and generic record combination
        String rg3 = "rg3" + stream;
        readerGroupManager.createReaderGroup(rg3,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map2 = new HashMap<>();
        // add only two schemas
        map2.put(Test1.class, schema1);
        map2.put(Test2.class, schema2);

        Serializer<Either<SpecificRecordBase, GenericRecord>> eitherDeserializer = 
                SerializerFactory.avroTypedOrGenericDeserializer(serializerConfig, map2);

        EventStreamReader<Either<SpecificRecordBase, GenericRecord>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

        EventRead<Either<SpecificRecordBase, GenericRecord>> e1 = reader3.readNextEvent(10000);
        assertNotNull(e1.getEvent());
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof Test1);
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof Test2);
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isRight());
        
        reader.close();
        reader2.close();
        reader3.close();
        readerGroupManager.close();
        //endregion
    }

    @Test
    public void testProtobuf() throws IOException {
        testProtobuf(true);
        testProtobuf(false);
    }

    private void testProtobuf(boolean encodeHeaders) throws IOException {
        // create stream
        String scope = "scope";
        String stream = "protobuf" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Protobuf;

        Path path = Paths.get("resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Serializer<ProtobufTest.Message1> serializer = SerializerFactory.protobufSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<ProtobufTest.Message1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build()).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String readerGroupName = "rg" + stream;
        readerGroupManager.createReaderGroup(readerGroupName, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<ProtobufTest.Message1> deserializer = SerializerFactory.protobufDeserializer(serializerConfig, schema);

        EventStreamReader<ProtobufTest.Message1> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

        EventRead<ProtobufTest.Message1> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        reader.close();
        // endregion
        
        // region generic read
        // 1. try without passing the schema. writer schema will be used to read for encoding header and latest schema will be used for non encoded header
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        ProtobufSchema<DynamicMessage> readerSchema = encodeHeaders ? null : 
                ProtobufSchema.from(client.getLatestSchemaVersion(groupId, null).getSchemaInfo());
        Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, readerSchema);

        EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<DynamicMessage> event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());

        reader2.close();
        
        // 2. try with passing the schema. reader schema will be used to read
        String rg3 = "rg3" + encodeHeaders;
        readerGroupManager.createReaderGroup(rg3,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        ProtobufSchema<DynamicMessage> schema2 = ProtobufSchema.of(ProtobufTest.Message1.getDescriptor().getFullName(), descriptorSet);
        genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, schema2);

        reader2 = clientFactory.createReader("r1", rg3, genericDeserializer, ReaderConfig.builder().build());

        event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        
        reader2.close();
        // endregion

        if (encodeHeaders) {
            String rg4 = "rg4" + System.currentTimeMillis();
            readerGroupManager.createReaderGroup(rg4,
                    ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

            Serializer<String> jsonDes = SerializerFactory.deserializeAsJsonString(serializerConfig);

            EventStreamReader<String> jsonReader = clientFactory.createReader("r1", rg4, jsonDes, ReaderConfig.builder().build());

            EventRead<String> jsonEvent = jsonReader.readNextEvent(10000);
            assertNotNull(jsonEvent.getEvent());

            jsonReader.close();
        }
        readerGroupManager.close();
    }

    @Test
    public void testProtobufMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "protomultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Protobuf;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(), true));

        Path path = Paths.get("resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema3 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message3.class, descriptorSet);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message1.class, schema1);
        map.put(ProtobufTest.Message2.class, schema2);
        map.put(ProtobufTest.Message3.class, schema3);
        Serializer<GeneratedMessageV3> serializer = SerializerFactory.protobufMultiTypeSerializer(serializerConfig, map);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<GeneratedMessageV3> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build()).join();
        writer.writeEvent(ProtobufTest.Message2.newBuilder().setName("test").setField1(0).build()).join();
        writer.writeEvent(ProtobufTest.Message3.newBuilder().setName("test").setField1(0).setField2(1).build()).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<GeneratedMessageV3> deserializer = SerializerFactory.protobufMultiTypeDeserializer(serializerConfig, map);

        EventStreamReader<GeneratedMessageV3> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<GeneratedMessageV3> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message1);
        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message2);
        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message3);

        reader.close();
        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<DynamicMessage> genericDeserializer = SerializerFactory.protobufGenericDeserializer(serializerConfig, null);

        EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<DynamicMessage> genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(10000);
        
        reader2.close();
        // endregion

        // region read using multiplexed and generic record combination
        String rg3 = "rg3" + stream;
        readerGroupManager.createReaderGroup(rg3,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map2 = new HashMap<>();
        // add only two schemas
        map2.put(ProtobufTest.Message1.class, schema1);
        map2.put(ProtobufTest.Message2.class, schema2);

        Serializer<Either<GeneratedMessageV3, DynamicMessage>> eitherDeserializer =
                SerializerFactory.protobufTypedOrGenericDeserializer(serializerConfig, map2);

        EventStreamReader<Either<GeneratedMessageV3, DynamicMessage>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

        EventRead<Either<GeneratedMessageV3, DynamicMessage>> e1 = reader3.readNextEvent(10000);
        assertNotNull(e1.getEvent());
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof ProtobufTest.Message1);
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof ProtobufTest.Message2);
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isRight());
        
        reader3.close();
        readerGroupManager.close();
        //endregion
    }

    @Test
    public void testJson() throws IOException {
        testJson(true);
        testJson(false);
    }

    private void testJson(boolean encodeHeaders) throws IOException {
        // create stream
        String scope = "scope";
        String stream = "json" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Json;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(), 
                false, ImmutableMap.of(SerializerFactory.ENCODE, Boolean.toString(encodeHeaders))));
        
        JSONSchema<DerivedUser2> schema = JSONSchema.of(DerivedUser2.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Serializer<DerivedUser2> serializer = SerializerFactory.jsonSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<DerivedUser2> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new DerivedUser2("name", new Address("street", "city"), 30, "user2")).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String readerGroupName = "rg" + stream;
        readerGroupManager.createReaderGroup(readerGroupName, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<DerivedUser2> deserializer = SerializerFactory.jsonDeserializer(serializerConfig, schema);

        EventStreamReader<DerivedUser2> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

        EventRead<DerivedUser2> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        reader.close();
        // endregion
        
        // region generic read
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<JsonGenericObject> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

        EventStreamReader<JsonGenericObject> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<JsonGenericObject> event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        JsonGenericObject obj = event2.getEvent();
        
        com.fasterxml.jackson.module.jsonSchema.JsonSchema jsonSchema = obj.getJsonSchema();
        if (encodeHeaders) {
            assertNotNull(jsonSchema);
        } else {
            assertNull(jsonSchema);
        }
        reader2.close();
        readerGroupManager.close();
        // endregion
    }

    @Test
    public void testJsonMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "jsonmultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        streamManager.close();
        SerializationFormat serializationFormat = SerializationFormat.Json;

        JSONSchema<User> schema1 = JSONSchema.ofBaseType(DerivedUser1.class, User.class);
        JSONSchema<User> schema2 = JSONSchema.ofBaseType(DerivedUser2.class, User.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        // region writer
        Map<Class<? extends User>, JSONSchema<User>> map = new HashMap<>();
        map.put(DerivedUser1.class, schema1);
        map.put(DerivedUser2.class, schema2);
        Serializer<User> serializer = SerializerFactory.jsonMultiTypeSerializer(serializerConfig, map);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<User> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new DerivedUser2()).join();
        writer.writeEvent(new DerivedUser1()).join();
        writer.close();
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<User> deserializer = SerializerFactory.jsonMultiTypeDeserializer(serializerConfig, map);

        EventStreamReader<User> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<User> event = reader.readNextEvent(10000);
        
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof DerivedUser2);
        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof DerivedUser1);
        reader.close();
        // endregion

        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<JsonGenericObject> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

        EventStreamReader<JsonGenericObject> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<JsonGenericObject> genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(10000);
        assertNotNull(genEvent.getEvent());
        reader2.close();
        // endregion

        // region read using multiplexed and generic record combination
        String rg3 = "rg3" + stream;
        readerGroupManager.createReaderGroup(rg3,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Map<Class<? extends User>, JSONSchema<User>> map2 = new HashMap<>();
        // add only one schema
        map2.put(DerivedUser1.class, schema1);

        Serializer<Either<User, JsonGenericObject>> eitherDeserializer =
                SerializerFactory.jsonTypedOrGenericDeserializer(serializerConfig, map2);

        EventStreamReader<Either<User, JsonGenericObject>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

        EventRead<Either<User, JsonGenericObject>> e1 = reader3.readNextEvent(10000);
        assertNotNull(e1.getEvent());
        assertTrue(e1.getEvent().isRight());
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof DerivedUser1);
        reader3.close();
        readerGroupManager.close();
        //endregion
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class TestClass {
        private String test;
    }
}

