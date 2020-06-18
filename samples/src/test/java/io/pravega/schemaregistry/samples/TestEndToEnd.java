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
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public abstract class TestEndToEnd {
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
    
    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(10);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }
    
    @Test
    public void testEndToEnd() {
        SchemaStore store = getStore();
        SchemaRegistryService service = new SchemaRegistryService(store, executor);
        SchemaRegistryClient client = new PassthruSchemaRegistryClient(service);

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

        client.updateCompatibility(group, Compatibility.fullTransitive(), null);

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SerializationFormat.Avro,
                ByteBuffer.wrap(schema3.toString().getBytes(Charsets.UTF_8)), ImmutableMap.of());

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        CompletableFuture.supplyAsync(() -> client.addSchema(group, schemaInfo3))
                         .exceptionally(e -> {
                             exceptionRef.set(Exceptions.unwrap(e));
                             return null;
                         }).join();

        assertTrue(exceptionRef.get() instanceof IncompatibleSchemaException);

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
        EncodingId encodingId = client.getEncodingId(group, version2, CodecFactory.NONE);
        assertEquals(encodingId.getId(), 0);
        client.deleteSchemaVersion(group, version2);
        SchemaInfo schema = client.getSchemaForVersion(group, version2);
        assertEquals(schema, schemaInfo2);
        AssertExtensions.assertThrows("", () -> client.getVersionForSchema(group, schemaInfo2),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        encodingId = client.getEncodingId(group, version2, CodecFactory.NONE);
        assertEquals(encodingId.getId(), 0);
        AssertExtensions.assertThrows("", () -> client.getEncodingId(group, version2, CodecFactory.MIME_GZIP),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);

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
    }

    abstract SchemaStore getStore();

}

