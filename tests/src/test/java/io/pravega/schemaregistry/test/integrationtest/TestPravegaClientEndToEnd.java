/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest;

import com.google.common.collect.ImmutableList;
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
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerDeFactory;
import io.pravega.schemaregistry.service.IncompatibleSchemaException;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.test.integrationtest.generated.ProtobufTest;
import io.pravega.schemaregistry.test.integrationtest.generated.Test1;
import io.pravega.schemaregistry.test.integrationtest.generated.Test2;
import io.pravega.schemaregistry.test.integrationtest.generated.Test3;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
            .withDefault("backward compatible with schema1")
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

    private final LocalPravegaEmulator localPravega;
    private final ClientConfig clientConfig;

    private final SchemaStore schemaStore;
    private final ScheduledExecutorService executor;
    private final SchemaRegistryService service;
    private final SchemaRegistryClient client;

    public TestPravegaClientEndToEnd() throws Exception {
        executor = Executors.newScheduledThreadPool(10);
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator
                .builder()
                .controllerPort(9090)
                .segmentStorePort(1234)
                .zkPort(2180)
                .restServerPort(9091)
                .enableRestServer(false)
                .enableAuth(false)
                .enableTls(false);

        localPravega = emulatorBuilder.build();
        localPravega.getInProcPravegaCluster().start();

        clientConfig = ClientConfig.builder().controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI())).build();

        schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, executor);

        service = new SchemaRegistryService(schemaStore);
        client = new TestRegistryClient(service);
    }
    
    @Override
    @After
    public void close() throws Exception {
        localPravega.close();
        executor.shutdownNow();
    }
    
    @Test
    public void test() throws IOException {
        testCompression();
        testAvroSchemaEvolution();
        
        testAvroReflect();    
        testAvroGenerated();    
        testAvroMultiplexed();   
        
        testProtobuf(true);
        testProtobuf(false);
        testProtobufMultiplexed();
    }
    
    private void testAvroSchemaEvolution() {
        // create stream
        String scope = "scope";
        String stream = "avroevolution";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<GenericRecord> schema1 = AvroSchema.of(SCHEMA1);
        AvroSchema<GenericRecord> schema2 = AvroSchema.of(SCHEMA2);
        AvroSchema<GenericRecord> schema3 = AvroSchema.of(SCHEMA3);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerDeFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
        writer.writeEvent(record);
        // endregion
        
        // region writer with schema2
        serializer = SerDeFactory.avroSerializer(serializerConfig, schema2);

        writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
        writer.writeEvent(record);
        // endregion
        
        // region writer with schema3
        // this should throw exception as schema change is not backward compatible.
        boolean exceptionThrown = false;
        try {
            serializer = SerDeFactory.avroSerializer(serializerConfig, schema3);
        } catch (Exception ex) {
            exceptionThrown = Exceptions.unwrap(ex) instanceof IncompatibleSchemaException;
        }
        assertTrue(exceptionThrown);
        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<GenericRecord> readSchema = AvroSchema.of(SCHEMA2);

        Serializer<GenericRecord> deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, readSchema);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        // read two events successfully
        EventRead<GenericRecord> event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());

        // create new reader, this time with incompatible schema3
        readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg1 = "rg1" + stream;
        readerGroupManager.createReaderGroup(rg1, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        readSchema = AvroSchema.of(SCHEMA3);

        exceptionThrown = false;
        try {
            deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, readSchema);
        } catch (Exception ex) {
            exceptionThrown = Exceptions.unwrap(ex) instanceof IllegalArgumentException;
        }
        assertTrue(exceptionThrown);
        
        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, null);

        reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());

        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        // endregion
    }
    
    private void testCompression() {
        // create stream
        String scope = "scope";
        String stream = "avrocompression";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<GenericRecord> schema1 = AvroSchema.of(SCHEMA1);
        AvroSchema<GenericRecord> schema2 = AvroSchema.of(SCHEMA2);
        AvroSchema<Test1> schema3 = AvroSchema.of(Test1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // region writer with schema1
        Serializer<GenericRecord> serializer = SerDeFactory.avroSerializer(serializerConfig, schema1);

        EventStreamWriter<GenericRecord> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        GenericRecord record = new GenericRecordBuilder(SCHEMA1).set("a", "test").build();
        writer.writeEvent(record);
        // endregion
        
        // region writer with schema2
        serializer = SerDeFactory.avroSerializer(serializerConfig, schema2);

        writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        record = new GenericRecordBuilder(SCHEMA2).set("a", "test").set("b", "value").build();
        writer.writeEvent(record);
        // endregion
        
        // region writer with schema3
        String mycompression = "mycompression";
        serializerConfig = SerializerConfig.builder()
                                           .groupId(groupId)
                                           .autoRegisterSchema(true)
                                           .compressor(new Compressor() {
                                               @Override
                                               public CompressionType getCompressionType() {
                                                   return CompressionType.custom(mycompression);
                                               }

                                               @Override
                                               public ByteBuffer compress(ByteBuffer data) {
                                                   return data;
                                               }

                                               @Override
                                               public ByteBuffer uncompress(ByteBuffer data) {
                                                   return data;
                                               }
                                           })
                                           .registryConfigOrClient(Either.right(client))
                                           .build();

        Serializer<Test1> serializer2 = SerDeFactory.avroSerializer(serializerConfig, schema3);
        EventStreamWriter<Test1> writer2 = clientFactory.createEventWriter(stream, serializer2, EventWriterConfig.builder().build());
        writer2.writeEvent(new Test1("a", 1));

        List<CompressionType> list = client.getCompressions(groupId);
        assertEquals(2, list.size());
        assertTrue(list.stream().anyMatch(x -> mycompression.equals(x.getCustomTypeName())));
        assertTrue(list.stream().anyMatch(x -> x.equals(CompressionType.None)));
        // endregion
    }
    
    private void testAvroReflect() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avroreflect";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<TestClass> schema = AvroSchema.of(TestClass.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        
        // region writer
        Serializer<TestClass> serializer = SerDeFactory.avroSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<TestClass> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new TestClass("test"));

        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<GenericRecord> readSchema = AvroSchema.of(ReflectData.get().getSchema(TestClass.class));

        Serializer<GenericRecord> deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, readSchema);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());

        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        deserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, null);

        reader = clientFactory.createReader("r1", rg2, deserializer, ReaderConfig.builder().build());

        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        // endregion
    }
    
    private void testAvroGenerated() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avrogenerated";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<Test1> schema = AvroSchema.of(Test1.class);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Serializer<Test1> serializer = SerDeFactory.avroSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<Test1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new Test1("test", 0));

        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        AvroSchema<Test1> readSchema = AvroSchema.of(Test1.class);

        Serializer<Test1> deserializer = SerDeFactory.avroDeserializer(serializerConfig, readSchema);

        EventStreamReader<Test1> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<Test1> event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());

        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> genericDeserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, null);

        EventStreamReader<GenericRecord> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> event2 = reader2.readNextEvent(1000);
        assertNotNull(event2.getEvent());
        // endregion
    }
    
    private void testAvroMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "avromultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Avro;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<SpecificRecordBase> schema1 = AvroSchema.of(Test1.class, Test1.getClassSchema());
        AvroSchema<SpecificRecordBase> schema2 = AvroSchema.of(Test2.class, Test2.getClassSchema());
        AvroSchema<SpecificRecordBase> schema3 = AvroSchema.of(Test3.class, Test3.getClassSchema());

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Map<Class<? extends SpecificRecordBase>, AvroSchema<SpecificRecordBase>> map = new HashMap<>();
        map.put(Test1.class, schema1);
        map.put(Test2.class, schema2);
        map.put(Test3.class, schema3);
        Serializer<SpecificRecordBase> serializer = SerDeFactory.multiplexedAvroSerializer(serializerConfig, map);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<SpecificRecordBase> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(new Test1("test", 0));
        writer.writeEvent(new Test2("test", 0, "test"));
        writer.writeEvent(new Test3("test", 0, "test", "test"));

        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<SpecificRecordBase> deserializer = SerDeFactory.multiplexedAvroDeserializer(serializerConfig, map);

        EventStreamReader<SpecificRecordBase> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<SpecificRecordBase> event1 = reader.readNextEvent(1000);
        assertNotNull(event1.getEvent());
        assertTrue(event1.getEvent() instanceof Test1);
        EventRead<SpecificRecordBase> event2 = reader.readNextEvent(1000);
        assertNotNull(event2.getEvent());
        assertTrue(event2.getEvent() instanceof Test2);
        EventRead<SpecificRecordBase> event3 = reader.readNextEvent(1000);
        assertNotNull(event3.getEvent());
        assertTrue(event3.getEvent() instanceof Test3);

        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<GenericRecord> genericDeserializer = SerDeFactory.genericAvroDeserializer(serializerConfig, null);

        EventStreamReader<GenericRecord> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<GenericRecord> genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        // endregion
    }
    
    private void testProtobuf(boolean encodeHeaders) throws IOException {
        // create stream
        String scope = "scope";
        String stream = "protobuf" + encodeHeaders;
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Protobuf;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.AllowAny)), 
                true, encodeHeaders);

        Path path = Paths.get("resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<ProtobufTest.Message1> schema = ProtobufSchema.of(ProtobufTest.Message1.class, descriptorSet);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Serializer<ProtobufTest.Message1> serializer = SerDeFactory.protobufSerializer(serializerConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<ProtobufTest.Message1> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build());

        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String readerGroupName = "rg" + stream;
        readerGroupManager.createReaderGroup(readerGroupName, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<ProtobufTest.Message1> deserializer = SerDeFactory.protobufDeserializer(serializerConfig, schema);

        EventStreamReader<ProtobufTest.Message1> reader = clientFactory.createReader("r1", readerGroupName, deserializer, ReaderConfig.builder().build());

        EventRead<ProtobufTest.Message1> event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());

        // endregion
        
        // region generic read
        // 1. try without passing the schema. writer schema will be used to read
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<DynamicMessage> genericDeserializer = SerDeFactory.genericProtobufDeserializer(serializerConfig, null);

        EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<DynamicMessage> event2 = reader2.readNextEvent(1000);
        assertNotNull(event2.getEvent());

        // 2. try with passing the schema. reader schema will be used to read
        String rg3 = "rg3" + encodeHeaders;
        readerGroupManager.createReaderGroup(rg3,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        ProtobufSchema<DynamicMessage> schema2 = ProtobufSchema.of(ProtobufTest.Message1.class.getSimpleName(), descriptorSet);
        genericDeserializer = SerDeFactory.genericProtobufDeserializer(serializerConfig, schema2);

        reader2 = clientFactory.createReader("r1", rg3, genericDeserializer, ReaderConfig.builder().build());

        event2 = reader2.readNextEvent(1000);
        assertNotNull(event2.getEvent());
        // endregion
    }
    
    private void testProtobufMultiplexed() throws IOException {
        // create stream
        String scope = "scope";
        String stream = "protomultiplexed";
        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        SchemaType schemaType = SchemaType.Protobuf;
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.AllowAny)), 
                true, true);

        Path path = Paths.get("resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        ProtobufSchema<GeneratedMessageV3> schema1 = ProtobufSchema.of(ProtobufTest.Message1.class.getSimpleName(), 
                ProtobufTest.Message1.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema2 = ProtobufSchema.of(ProtobufTest.Message2.class.getSimpleName(), 
                ProtobufTest.Message2.class, descriptorSet);
        ProtobufSchema<GeneratedMessageV3> schema3 = ProtobufSchema.of(ProtobufTest.Message3.class.getSimpleName(), 
                ProtobufTest.Message3.class, descriptorSet);

        SerializerConfig serializerConfig = SerializerConfig.builder()
                                                            .groupId(groupId)
                                                            .autoRegisterSchema(true)
                                                            .registryConfigOrClient(Either.right(client))
                                                            .build();
        // region writer
        Map<Class<? extends GeneratedMessageV3>, ProtobufSchema<GeneratedMessageV3>> map = new HashMap<>();
        map.put(ProtobufTest.Message1.class, schema1);
        map.put(ProtobufTest.Message2.class, schema2);
        map.put(ProtobufTest.Message3.class, schema3);
        Serializer<GeneratedMessageV3> serializer = SerDeFactory.multiplexedProtobufSerializer(serializerConfig, map);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<GeneratedMessageV3> writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        writer.writeEvent(ProtobufTest.Message1.newBuilder().setName("test").setInternal(ProtobufTest.InternalMessage.newBuilder().setValue(ProtobufTest.InternalMessage.Values.val1).build()).build());
        writer.writeEvent(ProtobufTest.Message2.newBuilder().setName("test").setField1(0).build());
        writer.writeEvent(ProtobufTest.Message3.newBuilder().setName("test").setField1(0).setField2(1).build());

        // endregion

        // region read into specific schema
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        String rg = "rg" + stream;
        readerGroupManager.createReaderGroup(rg, 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());
        
        Serializer<GeneratedMessageV3> deserializer = SerDeFactory.multiplexedProtobufDeserializer(serializerConfig, map);

        EventStreamReader<GeneratedMessageV3> reader = clientFactory.createReader("r1", rg, deserializer, ReaderConfig.builder().build());

        EventRead<GeneratedMessageV3> event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message1);
        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message2);
        event = reader.readNextEvent(1000);
        assertNotNull(event.getEvent());
        assertTrue(event.getEvent() instanceof ProtobufTest.Message3);

        // endregion
        // region read into writer schema
        String rg2 = "rg2" + stream;
        readerGroupManager.createReaderGroup(rg2,
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        Serializer<DynamicMessage> genericDeserializer = SerDeFactory.genericProtobufDeserializer(serializerConfig, null);

        EventStreamReader<DynamicMessage> reader2 = clientFactory.createReader("r1", rg2, genericDeserializer, ReaderConfig.builder().build());

        EventRead<DynamicMessage> genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        genEvent = reader2.readNextEvent(1000);
        assertNotNull(genEvent.getEvent());
        // endregion
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

