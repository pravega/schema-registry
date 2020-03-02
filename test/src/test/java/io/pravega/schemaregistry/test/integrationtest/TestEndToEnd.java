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
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

@Slf4j
public abstract class TestEndToEnd {

    protected ScheduledExecutorService executor;
    
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
        SchemaRegistryService service = new SchemaRegistryService(store);
        SchemaRegistryClient client = new TestRegistryClient(service);

        assertEquals(client.listNamespaces().size(), 0);

        String namespace = "namespace";
        String group = "group";
        client.createNamespace(namespace);
        assertEquals(client.listNamespaces().size(), 1);

        assertEquals(client.listGroups(namespace).size(), 0);
        
        client.addGroup(namespace, group, SchemaType.of(SchemaType.Type.Avro),  
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.AllowAny)), 
                true, true);
        assertEquals(client.listGroups(namespace).size(), 1);

        Schema schema = ReflectData.get().getSchema(TestClass.class); 
        SchemaInfo schemaInfo = new SchemaInfo(TestClass.class.getSimpleName(), SchemaType.of(SchemaType.Type.Avro), 
                schema.toString().getBytes(), ImmutableMap.of());

        client.addSchemaIfAbsent(namespace, group, schemaInfo, SchemaValidationRules.of());

        // attempt to add an existing schema
        client.addSchemaIfAbsent(namespace, group, schemaInfo, SchemaValidationRules.of());

        Schema schema2 = ReflectData.get().getSchema(TestClass2.class);
        SchemaInfo schemaInfo2 = new SchemaInfo(TestClass.class.getSimpleName(), SchemaType.of(SchemaType.Type.Avro),
                schema2.toString().getBytes(), ImmutableMap.of());
        client.addSchemaIfAbsent(namespace, group, schemaInfo2, SchemaValidationRules.of());

        client.updateSchemaValidationRules(namespace, group, new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)));

        Schema schema3 = ReflectData.get().getSchema(TestClass3.class);
        SchemaInfo schemaInfo3 = new SchemaInfo(TestClass.class.getSimpleName(), SchemaType.of(SchemaType.Type.Avro),
                schema3.toString().getBytes(), ImmutableMap.of());
        client.addSchemaIfAbsent(namespace, group, schemaInfo3, SchemaValidationRules.of());

        log.info("Schema Evolution History:", client.getGroupEvolutionHistory(namespace, group, TestClass.class.getSimpleName()));
    }

    abstract SchemaStore getStore();

    @Data
    public static class TestClass {
        private final String test;
    }
    
    @Data
    public static class TestClass2 {
        private final String test;
    }

    @Data
    public static class TestClass3 {
        private final String test;
    }
}
