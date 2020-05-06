/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class SchemaRegistryServiceTest {
    private SchemaRegistryService service;
    private ScheduledExecutorService executor;
    private SchemaStore store;
    @Before
    public void setup() {

        executor = Executors.newScheduledThreadPool(5);
        store = mock(SchemaStore.class);
        service = new SchemaRegistryService(store, executor);
    }
    
    @After
    public void teardown() {
        executor.shutdownNow();
    }

    @Test
    public void testListGroups() {
        ArrayList<String> groups = Lists.newArrayList("grp1", "grp2");
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new ListWithToken<>(groups, null));
        }).when(store).listGroups(any());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Avro,
                    SchemaValidationRules.of(Compatibility.backward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());

        MapWithToken<String, GroupProperties> result = service.listGroups(null).join();
        assertEquals(result.getMap().size(), 2);
    }

    @Test
    public void testCreateGroup(){
        GroupProperties groupProperties = new GroupProperties(SchemaType.Avro, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false"));
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(true));
        }).when(store).createGroup(anyString(),any());
        Boolean ans = service.createGroup("mygroup", groupProperties).join();
        assertEquals(new Boolean(true), ans);
        // already exists
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(false));
        }).when(store).createGroup(anyString(),any());
        ans = service.createGroup("mygroup", groupProperties).join();
        assertEquals(new Boolean(false), ans);
    }

    @Test
    public void testGetGroupProperties(){
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Avro, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        GroupProperties groupProperties = service.getGroupProperties("mygroup").join();
        assertEquals(SchemaType.Avro, groupProperties.getSchemaType());
        assertEquals(SchemaValidationRules.of(Compatibility.forward()), groupProperties.getSchemaValidationRules());
        assertEquals(Boolean.FALSE, groupProperties.isValidateByObjectType());
        assertEquals(Collections.singletonMap("Encode", "false"), groupProperties.getProperties());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.getGroupProperties("mygroup"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testUpdateSchemaValidationRules() throws ExecutionException, InterruptedException {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(null);
        }).when(store).updateValidationRules(anyString(), any(), any());
        /*doAnswer(x -> {
            return CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5));
        }).when(store).getGroupEtag(anyString());
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()), null).join();
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Avro, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());*/
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()), SchemaValidationRules.of(Compatibility.forward()));

        // PreconditionFailed Exception
        /*doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.forward()), SchemaValidationRules.of(Compatibility.forward())), e -> e instanceof PreconditionFailedException);*/
        // Runtime Exception
        doAnswer(x -> (new RuntimeException())).when(store).getGroupEtag(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.forward()), SchemaValidationRules.of(Compatibility.forward())), e -> Exceptions.unwrap(e) instanceof RuntimeException);
    }

    @Test
    public void testGetObjectTypes(){
        List<String> stringList = new ArrayList<>();
        stringList.add("objectType1");
        stringList.add("objectType2");
        ContinuationToken continuationToken  = new ContinuationToken();
        ListWithToken<String> listWithToken = new ListWithToken<>(stringList, continuationToken);
        doAnswer(x -> CompletableFuture.completedFuture(listWithToken)).when(store).listObjectTypes(anyString(), any());
        ListWithToken<String> stringListWithToken = service.getObjectTypes("mygroup", continuationToken).join();
        assertEquals(2, stringListWithToken.getList().size());
        // Runtime Exception
        doAnswer(x -> (new RuntimeException())).when(store).listObjectTypes(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getObjectTypes("mygroup", continuationToken), e -> e instanceof RuntimeException);
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).listObjectTypes(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getObjectTypes("mygroup", continuationToken), e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testAddSchema(){
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5));
        }).when(store).getGroupEtag(anyString());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Protobuf, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        // SchemaTypeMismatch
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", "anyObject", SchemaType.Avro, schemaData,
                Collections.singletonMap("key", "value"));
        AssertExtensions.assertThrows("An exception should have been thrown", () -> service.addSchema("mygroup", schemaInfo), e -> Exceptions.unwrap(e) instanceof SchemaTypeMismatchException);

    }

}


















