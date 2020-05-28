/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
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
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public abstract class TestEndToEnd {
    protected ScheduledExecutorService executor;

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

        int groupsCount = client.listGroups().size();
        
        client.addGroup(group, SerializationFormat.Avro,  
                SchemaValidationRules.of(Compatibility.backward()), 
                true, Collections.emptyMap());
        assertEquals(client.listGroups().size(), groupsCount + 1);

        String myTest = "MyTest";
        SchemaInfo schemaInfo = new SchemaInfo(myTest, SerializationFormat.Avro, 
                schema1.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        VersionInfo version1 = client.addSchema(group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getOrdinal(), 0);
        assertEquals(version1.getType(), myTest);
        // attempt to add an existing schema
        version1 = client.addSchema(group, schemaInfo);
        assertEquals(version1.getVersion(), 0);
        assertEquals(version1.getOrdinal(), 0);
        assertEquals(version1.getType(), myTest);

        SchemaInfo schemaInfo2 = new SchemaInfo(myTest, SerializationFormat.Avro,
                schema2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version2 = client.addSchema(group, schemaInfo2);
        assertEquals(version2.getVersion(), 1);
        assertEquals(version2.getOrdinal(), 1);
        assertEquals(version2.getType(), myTest);

        client.updateSchemaValidationRules(group, SchemaValidationRules.of(Compatibility.fullTransitive()));

        SchemaInfo schemaInfo3 = new SchemaInfo(myTest, SerializationFormat.Avro,
                schema3.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());

        AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        CompletableFuture.supplyAsync(() -> client.addSchema(group, schemaInfo3))
                         .exceptionally(e -> {
                             exceptionRef.set(Exceptions.unwrap(e));
                             return null;
                         }).join();
        
        assertTrue(exceptionRef.get() instanceof IncompatibleSchemaException);
        
        String myTest2 = "MyTest2";
        SchemaInfo schemaInfo4 = new SchemaInfo(myTest2, SerializationFormat.Avro,
                schemaTest2.toString().getBytes(Charsets.UTF_8), ImmutableMap.of());
        VersionInfo version3 = client.addSchema(group, schemaInfo4);
        assertEquals(version3.getVersion(), 0);
        assertEquals(version3.getOrdinal(), 2);
        assertEquals(version3.getType(), myTest2);

        List<String> types = client.getSchemas(group).stream().map(x -> x.getSchema().getType()).collect(Collectors.toList());
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
        EncodingId encodingId = client.getEncodingId(group, version2, CodecType.None);
        assertEquals(encodingId.getId(), 0);
        client.deleteSchemaVersion(group, version2);
        SchemaInfo schema = client.getSchemaForVersion(group, version2);
        assertEquals(schema, schemaInfo2);
        AssertExtensions.assertThrows("", () -> client.getVersionForSchema(group, schemaInfo2), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        encodingId = client.getEncodingId(group, version2, CodecType.None);
        assertEquals(encodingId.getId(), 0);
        AssertExtensions.assertThrows("", () -> client.getEncodingId(group, version2, CodecType.GZip),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        
        groupEvolutionHistory = client.getGroupHistory(group);
        assertEquals(groupEvolutionHistory.size(), 2);

        myTestHistory = client.getSchemaVersions(group, myTest);
        assertEquals(myTestHistory.size(), 1);
        SchemaWithVersion schemaWithVersion = client.getLatestSchemaVersion(group, myTest);
        assertEquals(schemaWithVersion.getVersion(), version1);
        
        schemaWithVersion = client.getLatestSchemaVersion(group, null);
        assertEquals(schemaWithVersion.getVersion(), version3);
        
        // add the schema again. it should get a new version
        VersionInfo version4 = client.addSchema(group, schemaInfo2);
        assertEquals(version4.getOrdinal(), 3);
        assertEquals(version4.getVersion(), 2);
    }

    abstract SchemaStore getStore();

}

