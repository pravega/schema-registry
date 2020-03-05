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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.PravegaSerDe;
import io.pravega.schemaregistry.serializers.SerDeConfig;
import io.pravega.schemaregistry.serializers.SerDeFactory;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class TestPravegaClientEndToEnd {
    private final Schema schema1 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private final Schema schema2 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .withDefault("backward compatible with schema1")
            .endRecord();

    private final Schema schema3 = SchemaBuilder
            .record("MyTest")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    private final Schema schemaTest2 = SchemaBuilder
            .record("MyTest2")
            .fields()
            .name("a")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("b")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();
    
    private SchemaStore schemaStore;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
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
    }
    
    @After
    public void tearDown() throws Exception {
        localPravega.close();

        executor.shutdownNow();
    }

    LocalPravegaEmulator localPravega;
    ClientConfig clientConfig;

    @Test
    public void testEndToEnd() throws IOException {
        // create stream
        StreamManager streamManager = new StreamManagerImpl(clientConfig);
        String scope = "scope";
        streamManager.createScope(scope);
        String stream = "stream";
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        
        SchemaRegistryService service = new SchemaRegistryService(schemaStore);
        SchemaRegistryClient client = new TestRegistryClient(service);

        String groupId = GroupIdGenerator.getGroupId(GroupIdGenerator.Type.QualifiedStreamName, scope, stream);

        SchemaType schemaType = SchemaType.of(SchemaType.Type.Avro);
        client.addGroup(groupId, schemaType,  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                true, true);

        AvroSchema<TestClass> schema = AvroSchema.of(TestClass.class);

        SerDeConfig serDeConfig = SerDeConfig.builder()
                                             .schemaType(schemaType)
                                             .groupId(groupId)
                                             .autoRegisterSchema(true)
                                       .build();
        
        PravegaSerDe<TestClass> serDe = SerDeFactory.createSerDe(client, serDeConfig, schema);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        EventStreamWriter<TestClass> writer = clientFactory.createEventWriter(stream, serDe, EventWriterConfig.builder().build());
        writer.writeEvent(new TestClass("test"));
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, clientConfig, new ConnectionFactoryImpl(clientConfig));
        readerGroupManager.createReaderGroup("rg", 
                ReaderGroupConfig.builder().stream(StreamSegmentNameUtils.getScopedStreamName(scope, stream)).disableAutomaticCheckpoints().build());

        PravegaSerDe<TestClass> genericSerde = SerDeFactory.createGenericDeserializer(client, serDeConfig, schema);

        EventStreamReader<GenericRecord> reader = clientFactory.createReader("r1", "rg", serDe, ReaderConfig.builder().build());

        EventRead<TestClass> event = reader.readNextEvent(1000);
        System.err.println(event.getEvent());
        
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

