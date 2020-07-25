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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.Codecs;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.samples.demo.objects.Address;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser1;
import io.pravega.schemaregistry.samples.demo.objects.DerivedUser2;
import io.pravega.schemaregistry.samples.demo.objects.User;
import io.pravega.schemaregistry.samples.generated.ProtobufTest;
import io.pravega.schemaregistry.samples.generated.Test1;
import io.pravega.schemaregistry.samples.generated.Test2;
import io.pravega.schemaregistry.samples.generated.Test3;
import io.pravega.schemaregistry.samples.generated.Type1;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.serializers.WithSchema;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
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

import java.io.ByteArrayOutputStream;
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
import java.util.function.Function;

import static org.junit.Assert.*;

@Slf4j
public class TestPravegaClientEndToEnd implements AutoCloseable {
    private static final Schema SCHEMA1 = SchemaBuilder
            .record("MyTest")
            .namespace("a.b.c")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private static final Schema SCHEMA2 = SchemaBuilder
            .record("MyTest")
            .namespace("a.b.c")
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
            .namespace("a.b.c")
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

        schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, executor);

        service = new SchemaRegistryService(schemaStore, executor);
        port = TestUtils.getAvailableListenPort();
        ServiceConfig serviceConfig = ServiceConfig.builder().port(port).build();

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();
        client =  SchemaRegistryClientFactory.withDefaultNamespace(
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
    public void testRestApis() {
        // create stream
        String groupId = "myGroup";
        SchemaInfo schemaInfo = AvroSchema.of(SCHEMA1).getSchemaInfo();
        SchemaInfo schemaInfo1 = AvroSchema.of(SCHEMA2).getSchemaInfo();
        SchemaInfo schemaInfo2 = AvroSchema.of(SCHEMA3).getSchemaInfo();

        Map<String, VersionInfo> references = client.getSchemaReferences(schemaInfo);
        int preTestCount = references.size();
        
        client.addGroup(groupId, new GroupProperties(SerializationFormat.Avro, Compatibility.allowAny(), true));
        client.addSchema(groupId, schemaInfo);
        client.addSchema(groupId, schemaInfo1);
        client.addSchema(groupId, schemaInfo2);

        VersionInfo version1 = client.getVersionForSchema(groupId, schemaInfo);
        assertEquals(version1.getId(), 0);
        VersionInfo version2 = client.getVersionForSchema(groupId, schemaInfo1);
        assertEquals(version2.getId(), 1);
        VersionInfo version3 = client.getVersionForSchema(groupId, schemaInfo2);
        assertEquals(version3.getId(), 2);

        references = client.getSchemaReferences(schemaInfo);
        assertEquals(references.size(), preTestCount + 1);

        String groupId2 = "mygrp2";
        client.addGroup(groupId2, new GroupProperties(SerializationFormat.Avro, Compatibility.allowAny(), true));
        client.addSchema(groupId2, schemaInfo);

        references = client.getSchemaReferences(schemaInfo);
        assertEquals(references.size(), preTestCount + 2);
    }
    
    @Test
    public void testAvroSchemaEvolution() {
        // create stream
        String scope = "scope";
        String stream = "avroevolution";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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
        
        AvroSchema<Object> readSchema = AvroSchema.of(SCHEMA2);

        Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

        EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        // read two events successfully
        EventRead<Object> event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());
        event = reader.readNextEvent(10000);
        assertNotNull(event.getEvent());

        reader.close();
        // create new reader, this time with incompatible schema3
        
        String rg1 = "rg1" + stream;
        readerGroupManager.createReaderGroup(rg1, 
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        AvroSchema<Object> readSchemaEx = AvroSchema.of(SCHEMA3);
        
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
        
        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test
    public void testCodec() {
        // create stream
        String scope = "scope";
        String stream = "avrocodec";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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
                                           .encoder(Codecs.GzipCompressor.getCodec())
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer3 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer3 = clientFactory.createEventWriter(stream, serializer3, EventWriterConfig.builder().build());
        String bigString = generateBigString(1);
        writer3.writeEvent(new Test1(bigString, 1)).join();

        List<CodecType> list = client.getCodecTypes(groupId);
        assertEquals(2, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.None.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.GzipCompressor.getCodec().getCodecType())));
        // endregion
        
        // region writer with codec snappy
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registerSchema(true)
                                           .registerCodec(true)
                                           .encoder(Codecs.SnappyCompressor.getCodec())
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer4 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer4 = clientFactory.createEventWriter(stream, serializer4, EventWriterConfig.builder().build());
        String bigString2 = generateBigString(200);
        writer4.writeEvent(new Test1(bigString2, 1)).join();

        list = client.getCodecTypes(groupId);
        assertEquals(3, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.None.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.GzipCompressor.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.SnappyCompressor.getCodec().getCodecType())));
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

        Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<Object> event = reader.readNextEvent(10000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            Object e = event.getEvent();
            event = reader.readNextEvent(10000);
        }
        // endregion

        // region writer with custom codec
        CodecType mycodec = new CodecType("mycodec");
        Codec myCodec = new Codec() {
            @Override
            public String getName() {
                return mycodec.getName();
            }

            @Override
            public CodecType getCodecType() {
                return mycodec;
            }

            @SneakyThrows
            @Override
            public void encode(ByteBuffer data, ByteArrayOutputStream bos) {
                bos.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
            }

            @SneakyThrows
            @Override
            public ByteBuffer decode(ByteBuffer data, Map<String, String> properties) {
                return data;
            }
        };
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .registerSchema(true)
                                           .registerCodec(true)
                                           .encoder(myCodec)
                                           .registryClient(client)
                                           .build();

        Serializer<Test1> serializer5 = SerializerFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer2 = clientFactory.createEventWriter(stream, serializer5, EventWriterConfig.builder().build());
        String bigString3 = generateBigString(300);
        writer2.writeEvent(new Test1(bigString3, 1)).join();
        // endregion 

        list = client.getCodecTypes(groupId);
        assertEquals(4, list.size());
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.None.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.GzipCompressor.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(Codecs.SnappyCompressor.getCodec().getCodecType())));
        assertTrue(list.stream().anyMatch(x -> x.equals(mycodec)));
        reader.close();

        // region new reader with additional codec
        // add new decoder for custom serialization
        SerializerConfig serializerConfig2 = SerializerConfig.builder()
                                                             .groupId(groupId)
                                                             .decoder(myCodec.getName(), myCodec)
                                                             .registryClient(client)
                                                             .build();

        Serializer<Object> deserializer2 = SerializerFactory.avroGenericDeserializer(serializerConfig2, null);

        EventStreamReader<Object> reader2 = clientFactory.createReader("r2", rg, deserializer2, ReaderConfig.builder().build());

        event = reader2.readNextEvent(10000);
        while (event.isCheckpoint() || event.getEvent() != null) {
            Object e = event.getEvent();
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

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
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
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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

        AvroSchema<Object> readSchema = AvroSchema.of(ReflectData.get().getSchema(TestClass.class));

        Serializer<Object> deserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, readSchema);

        EventStreamReader<Object> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<Object> event = reader.readNextEvent(10000);
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

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test
    public void testAvroGenerated() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avrogenerated";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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

        Serializer<Object> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<Object> event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        readerGroupManager.close();
        // endregion

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test
    public void testAvroMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avromultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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

        Serializer<Object> genericDeserializer = SerializerFactory.avroGenericDeserializer(serializerConfig, null);

        EventStreamReader<Object> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<Object> genEvent = reader2.readNextEvent(10000);
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

        Serializer<Either<SpecificRecordBase, Object>> eitherDeserializer = 
                SerializerFactory.avroTypedOrGenericDeserializer(serializerConfig, map2);

        EventStreamReader<Either<SpecificRecordBase, Object>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

        EventRead<Either<SpecificRecordBase, Object>> e1 = reader3.readNextEvent(10000);
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

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
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
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        SerializationFormat serializationFormat = SerializationFormat.Protobuf;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(), false, ImmutableMap.of()));

        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .writeEncodingHeader(encodeHeaders)
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

        Path path = Paths.get("resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

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

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test
    public void testProtobufMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "protomultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        SerializationFormat serializationFormat = SerializationFormat.Protobuf;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(), true));

        ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message1.class);
        ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message2.class);
        ProtobufSchema<GeneratedMessageV3> schema3 = ProtobufSchema.ofGeneratedMessageV3(ProtobufTest.Message3.class);

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

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
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
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        SerializationFormat serializationFormat = SerializationFormat.Json;
        client.addGroup(groupId, new GroupProperties(serializationFormat,
                Compatibility.allowAny(), false, ImmutableMap.of()));
        
        JSONSchema<DerivedUser2> schema = JSONSchema.of(DerivedUser2.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(serializationFormat,
                                                                    Compatibility.allowAny(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .writeEncodingHeader(encodeHeaders)
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

        Serializer<WithSchema<JsonNode>> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

        EventStreamReader<WithSchema<JsonNode>> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<WithSchema<JsonNode>> event2 = reader2.readNextEvent(10000);
        assertNotNull(event2.getEvent());
        WithSchema<JsonNode> obj = event2.getEvent();
        
        assertEquals(obj.hasJsonSchema(), encodeHeaders);
        if (encodeHeaders) {
            assertNotNull(obj.getJsonSchema());
        } 
        reader2.close();
        readerGroupManager.close();
        // endregion

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test
    public void testJsonMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "jsonmultiplexed";
        String groupId = NameUtils.getScopedStreamName(scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
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

        Serializer<WithSchema<JsonNode>> genericDeserializer = SerializerFactory.jsonGenericDeserializer(serializerConfig);

        EventStreamReader<WithSchema<JsonNode>> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<WithSchema<JsonNode>> genEvent = reader2.readNextEvent(10000);
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

        Serializer<Either<User, WithSchema<JsonNode>>> eitherDeserializer =
                SerializerFactory.jsonTypedOrGenericDeserializer(serializerConfig, map2);

        EventStreamReader<Either<User, WithSchema<JsonNode>>> reader3 = clientFactory.createReader("r1", rg3, eitherDeserializer, ReaderConfig.builder().build());

        EventRead<Either<User, WithSchema<JsonNode>>> e1 = reader3.readNextEvent(10000);
        assertNotNull(e1.getEvent());
        assertTrue(e1.getEvent().isRight());
        e1 = reader3.readNextEvent(10000);
        assertTrue(e1.getEvent().isLeft());
        assertTrue(e1.getEvent().getLeft() instanceof DerivedUser1);
        reader3.close();
        readerGroupManager.close();
        //endregion

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }

    @Test 
    @SneakyThrows
    public void testMultiFormatSerializerAndDeserializer() {
        String scope = "multi";
        String stream = "multi";
        String groupId = "multi";
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        // region avro
        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .createGroup(SerializationFormat.Any, Compatibility.allowAny(), true)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();

        AvroSchema<Type1> avro = AvroSchema.of(Type1.class);
        Serializer<Type1> serializer = SerializerFactory.avroSerializer(serializerConfig, avro);
        EventStreamWriter<Type1> avroWriter = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        avroWriter.writeEvent(new Type1("a", 1)).join();
        // endregion

        // region proto
        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class);
        Serializer<ProtobufTest.Message1> serializer2 = SerializerFactory.protobufSerializer(serializerConfig, schema);

        EventStreamWriter<ProtobufTest.Message1> protoWriter = clientFactory
                .createEventWriter(stream, serializer2, EventWriterConfig.builder().build());

        ProtobufTest.Message1 type1 = ProtobufTest.Message1.newBuilder().setName("test")
                                                           .setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val3).build())
                                                           .build();
        protoWriter.writeEvent(type1).join();
        // endregion
        
        // region write json
        JSONSchema<DerivedUser1> jsonSchema = JSONSchema.of(DerivedUser1.class);
        Serializer<DerivedUser1> serializer3 = SerializerFactory.jsonSerializer(serializerConfig, jsonSchema);
        // endregion

        EventStreamWriter<DerivedUser1> jsonWriter = clientFactory.createEventWriter(stream, serializer3, EventWriterConfig.builder().build());
        jsonWriter.writeEvent(new DerivedUser1("json", new Address("a", "b"), 1, "users")).join();

        // read using multiformat deserializer
        Serializer<WithSchema<Object>> deserializer = SerializerFactory.deserializerWithSchema(serializerConfig);
        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream + System.currentTimeMillis();
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        EventStreamReader<WithSchema<Object>> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());
        
        // read 3 events
        EventRead<WithSchema<Object>> event1 = reader.readNextEvent(1000);
        String jsonString = event1.getEvent().getJsonString();
        assertTrue(event1.getEvent().getObject() instanceof GenericRecord);
        EventRead<WithSchema<Object>> event2 = reader.readNextEvent(1000);
        assertTrue(event2.getEvent().getObject() instanceof DynamicMessage);
        EventRead<WithSchema<Object>> event3 = reader.readNextEvent(1000);
        assertTrue(event3.getEvent().getObject() instanceof JsonNode);

        // write using genericserializer
        Serializer<WithSchema<Object>> genericSerializer = SerializerFactory.serializerWithSchema(serializerConfig);

        EventStreamWriter<WithSchema<Object>> genericWriter = clientFactory
                .createEventWriter(stream, genericSerializer, EventWriterConfig.builder().build());
        
        genericWriter.writeEvent(event1.getEvent());
        genericWriter.writeEvent(event2.getEvent());
        genericWriter.writeEvent(event3.getEvent());
        // endregion

        // read these events back
        event1 = reader.readNextEvent(1000);
        assertTrue(event1.getEvent().getObject() instanceof GenericRecord);
        event2 = reader.readNextEvent(1000);
        assertTrue(event2.getEvent().getObject() instanceof DynamicMessage);
        event3 = reader.readNextEvent(1000);
        assertTrue(event3.getEvent().getObject() instanceof JsonNode);

        client.removeGroup(groupId);
        streamManager.sealStream(scope, stream);
        streamManager.deleteStream(scope, stream);
        streamManager.close();
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class TestClass {
        private String test;
    }

    @Test
    public void testWithSchema() {
        String scope = "withSchema";
        String stream = "withSchema";

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .namespace(scope)
                                                            .groupId(stream)
                                                            .createGroup(SerializationFormat.Json,
                                                                    Compatibility.allowAny(),
                                                                    false)
                                                            .registerSchema(true)
                                                            .registryClient(client)
                                                            .build();
        
        // region write
        Serializer<TestClass> serializer = SerializerFactory.jsonSerializer(serializerConfig, JSONSchema.of(TestClass.class));

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<TestClass> streamWriter = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        streamWriter.writeEvent(new TestClass("a")).join();
        // endregion
        
        // region read
        Serializer<WithSchema<Object>> deserializer = SerializerFactory.deserializerWithSchema(serializerConfig);
        Function<EventRead, String> eventContentSupplier = x -> ((WithSchema) x.getEvent()).getJsonString();

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg,
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        EventStreamReader<WithSchema<Object>> reader = clientFactory.createReader("r", rg, deserializer, ReaderConfig.builder().build());
        EventRead<WithSchema<Object>> eventRead = reader.readNextEvent(1000L); 
        String eventContent = eventContentSupplier.apply(eventRead); 
        assertFalse(Strings.isNullOrEmpty(eventContent));
        reader.close();
        
        streamWriter.writeEvent(new TestClass("a")).join();
        Serializer<String> deserializer2 = SerializerFactory.deserializeAsJsonString(serializerConfig);
        EventStreamReader<String> reader2 = clientFactory.createReader("r2", rg, deserializer2, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(1000L); 
        assertFalse(Strings.isNullOrEmpty(eventRead2.getEvent()));
        reader2.close();
        
        streamWriter.writeEvent(new TestClass("a")).join();
        Serializer<String> deserializer3 = SerializerFactory.deserializeAsT(serializerConfig, (x, y) -> y.toString());
        EventStreamReader<String> reader3 = clientFactory.createReader("r3", rg, deserializer3, ReaderConfig.builder().build());
        EventRead<String> eventRead3 = reader3.readNextEvent(1000L); 
        assertFalse(Strings.isNullOrEmpty(eventRead3.getEvent()));
    }
}

