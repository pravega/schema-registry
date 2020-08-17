/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.integrationtest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.codec.Codecs;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
public abstract class TestEndToEnd {
    public static final Random RANDOM = new Random();
    @Rule
    public Timeout globalTimeout = new Timeout(3, TimeUnit.MINUTES);

    ScheduledExecutorService executor;

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
            .withDefault("backwardPolicy compatible with schema1")
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
    
    private int port;
    private RestServer restServer;
    
    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(10);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(Config.THREAD_POOL_SIZE);

        port = TestUtils.getAvailableListenPort();
        ServiceConfig serviceConfig = ServiceConfig.builder().port(port).build();
        SchemaStore store = getStore();

        SchemaRegistryService service = new SchemaRegistryService(store, executor);

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();
    }

    @After
    public void tearDown() {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
    }
    
    @Test
    public void testEndToEnd() {
        SchemaRegistryClient client = SchemaRegistryClientFactory.withDefaultNamespace(
                SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:" + port)).build());
        
        String group = "group";

        int groupsCount = Lists.newArrayList(client.listGroups()).size();

        client.addGroup(group, new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(),
                true));
        assertEquals(Lists.newArrayList(client.listGroups()).size(), groupsCount + 1);

        String myTest = "MyTest";
        SchemaInfo schemaInfo = new SchemaInfo(myTest, SerializationFormat.Avro,
                ByteBuffer.wrap(schema1.toString().getBytes(Charsets.UTF_8)), ImmutableMap.of());

        VersionInfo version1 = client.addSchema(group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getId(), 0);
        assertEquals(version1.getType(), myTest);
        // attempt to add an existing schema
        version1 = client.addSchema(group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getId(), 0);
        assertEquals(version1.getType(), myTest);

        SchemaInfo schemaInfo2 = new SchemaInfo(myTest, SerializationFormat.Avro,
                ByteBuffer.wrap(schema2.toString().getBytes(Charsets.UTF_8)), ImmutableMap.of());
        VersionInfo version2 = client.addSchema(group, schemaInfo2);
        assertEquals(version2.getVersion(), 1);
        assertEquals(version2.getId(), 1);
        assertEquals(version2.getType(), myTest);

        assertFalse(client.updateCompatibility(group, Compatibility.fullTransitive(), Compatibility.forward()));

        assertTrue(client.updateCompatibility(group, Compatibility.fullTransitive(), null));
        
        assertTrue(client.updateCompatibility(group, Compatibility.backward(), Compatibility.fullTransitive()));

        assertFalse(client.updateCompatibility(group, Compatibility.backward(), Compatibility.fullTransitive()));

        assertTrue(client.updateCompatibility(group, Compatibility.fullTransitive(), Compatibility.backward()));

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SerializationFormat.Avro,
                ByteBuffer.wrap(schema3.toString().getBytes(Charsets.UTF_8)), ImmutableMap.of());

        AssertExtensions.assertThrows("", () -> client.addSchema(group, schemaInfo3), 
            e -> Exceptions.unwrap(e) instanceof RegistryExceptions.SchemaValidationFailedException);

        String myTest2 = "MyTest2";
        SchemaInfo schemaInfo4 = new SchemaInfo(myTest2, SerializationFormat.Avro,
                ByteBuffer.wrap(schemaTest2.toString().getBytes(Charsets.UTF_8)), ImmutableMap.of());
        VersionInfo version3 = client.addSchema(group, schemaInfo4);
        assertEquals(version3.getVersion(), 0);
        assertEquals(version3.getId(), 2);
        assertEquals(version3.getType(), myTest2);

        List<String> types = client.getSchemas(group).stream().map(x -> x.getSchemaInfo().getType()).collect(Collectors.toList());
        assertEquals(types.size(), 2);
        assertTrue(types.contains(myTest));
        assertTrue(types.contains(myTest2));
        List<GroupHistoryRecord> groupEvolutionHistory = client.getGroupHistory(group);
        assertEquals(groupEvolutionHistory.size(), 3);
        List<SchemaWithVersion> myTestHistory = client.getSchemaVersions(group, myTest);
        assertEquals(myTestHistory.size(), 2);
        List<SchemaWithVersion> myTest2History = client.getSchemaVersions(group, myTest2);
        assertEquals(myTest2History.size(), 1);

        // delete schemainfo2
        EncodingId encodingId = client.getEncodingId(group, version2, Codecs.None.getCodec().getName());
        assertEquals(encodingId.getId(), 0);
        client.deleteSchemaVersion(group, version2);
        SchemaInfo schema = client.getSchemaForVersion(group, version2);
        assertEquals(schema, schemaInfo2);
        AssertExtensions.assertThrows("", () -> client.getVersionForSchema(group, schemaInfo2),
                e -> Exceptions.unwrap(e) instanceof RegistryExceptions.ResourceNotFoundException);
        encodingId = client.getEncodingId(group, version2, Codecs.None.getCodec().getName());
        assertEquals(encodingId.getId(), 0);
        AssertExtensions.assertThrows("", () -> client.getEncodingId(group, version2, Codecs.GzipCompressor.getCodec().getName()),
                e -> Exceptions.unwrap(e) instanceof RegistryExceptions.ResourceNotFoundException);

        groupEvolutionHistory = client.getGroupHistory(group);
        assertEquals(groupEvolutionHistory.size(), 2);

        myTestHistory = client.getSchemaVersions(group, myTest);
        assertEquals(myTestHistory.size(), 1);
        SchemaWithVersion schemaWithVersion = client.getLatestSchemaVersion(group, myTest);
        assertEquals(schemaWithVersion.getVersionInfo(), version1);

        schemaWithVersion = client.getLatestSchemaVersion(group, null);
        assertEquals(schemaWithVersion.getVersionInfo(), version3);

        // add the schema again. it should get a new version
        VersionInfo version4 = client.addSchema(group, schemaInfo2);
        assertEquals(version4.getId(), 3);
        assertEquals(version4.getVersion(), 2);
        
        client.removeGroup(group);
    }

    abstract SchemaStore getStore();
}

