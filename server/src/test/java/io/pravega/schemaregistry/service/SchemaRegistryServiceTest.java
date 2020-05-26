/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        SchemaStore store = mock(SchemaStore.class);
        SchemaRegistryService service = new SchemaRegistryService(store, executor);

        ArrayList<String> groups = Lists.newArrayList("grp1", "grp2");
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new ListWithToken<>(groups, null));
        }).when(store).listGroups(any(), anyInt());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Avro, 
                    SchemaValidationRules.of(Compatibility.backward()), false, Collections.emptyMap()));
        }).when(store).getGroupProperties(eq("grp1"));

        doAnswer(x -> {
            return Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "group prop not found"));
        }).when(store).getGroupProperties(eq("grp2"));

        MapWithToken<String, GroupProperties> result = service.listGroups(null, 100).join();
        assertEquals(result.getMap().size(), 2);
    }

    @Test
    public void testCreateGroup() {
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Avro,
                SchemaValidationRules.of(Compatibility.backwardTransitive()), false, Collections.singletonMap("Encode", "false"));
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(true));
        }).when(store).createGroup(anyString(), any());
        Boolean ans = service.createGroup("mygroup", groupProperties).join();
        assertEquals(new Boolean(true), ans);
        // already exists
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(false));
        }).when(store).createGroup(anyString(), any());
        ans = service.createGroup("mygroup", groupProperties).join();
        assertEquals(new Boolean(false), ans);
    }

    @Test
    public void testGetGroupProperties() {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Avro, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        GroupProperties groupProperties = service.getGroupProperties("mygroup").join();
        assertEquals(SerializationFormat.Avro, groupProperties.getSerializationFormat());
        assertEquals(SchemaValidationRules.of(Compatibility.forward()), groupProperties.getSchemaValidationRules());
        assertEquals(Collections.singletonMap("Encode", "false"), groupProperties.getProperties());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.getGroupProperties("mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testUpdateSchemaValidationRules() throws ExecutionException, InterruptedException {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(null);
        }).when(store).updateValidationRules(anyString(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5))).when(store).getGroupEtag(
                anyString());
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()), null).join();
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Avro, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()), SchemaValidationRules.of(Compatibility.forward()));
        doAnswer(x -> CompletableFuture.completedFuture(
                new GroupProperties(SerializationFormat.Avro, SchemaValidationRules.of(Compatibility.forward()), false,
                        Collections.singletonMap("Encode", "false")))).when(store).getGroupProperties(anyString());
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()), null).join();
        service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.backward()),
                SchemaValidationRules.of(Compatibility.forward())).join();
        // PreconditionFailed Exception
        doAnswer(x -> CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Avro, SchemaValidationRules.of
                (Compatibility.backward()), false, Collections.singletonMap("Encode", "false")))).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.forward()
                ), SchemaValidationRules.of(Compatibility.forward())).join(), e -> e instanceof PreconditionFailedException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).updateValidationRules(anyString(), any(), any());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateSchemaValidationRules("mygroup", SchemaValidationRules.of(Compatibility.forward()
                ), SchemaValidationRules.of(Compatibility.backward())).join()
                , e -> Exceptions.unwrap(e) instanceof
                        RuntimeException);
    }

    @Test
    public void testGetSchemas(){
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Custom, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemas(anyString());
        List<SchemaWithVersion> schemaWithVersionList = service.getSchemas("mygroup").join();
        assertEquals(1, schemaWithVersionList.size());
        // Runtime Exception
        doAnswer(x -> (new RuntimeException())).when(store).getLatestSchemas(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchemas("mygroup"), e -> e instanceof RuntimeException);
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getLatestSchemas(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchemas("mygroup"), e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testAddSchema() {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5));
        }).when(store).getGroupEtag(anyString());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Protobuf, SchemaValidationRules.of(Compatibility.forward()), false, Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).addSchema(anyString(), any(),
                any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).getSchemaVersion(anyString(), any());
        VersionInfo versionInfo1 = service.addSchema("mygroup", schemaInfo).join();
        assertEquals(7, versionInfo1.getOrdinal());

        // SerializationFormatMismatch Exception
        doAnswer(x -> CompletableFuture.completedFuture(
                new GroupProperties(SerializationFormat.Avro, SchemaValidationRules.of(Compatibility.forward()), false,
                        Collections.singletonMap("Encode", "false")))).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown", () -> service.addSchema("mygroup", schemaInfo).join(), e -> e instanceof SerializationFormatMismatchException);

        // IncompatibleSchema Exception
        doAnswer(x -> CompletableFuture.completedFuture(
                new GroupProperties(SerializationFormat.Protobuf, SchemaValidationRules.of(Compatibility.forward()), false,
                        Collections.singletonMap("Encode", "false")))).when(store).getGroupProperties(anyString());
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getSchemaVersion(anyString(),any());
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        List<SchemaWithVersion> schemaWithVersionList = new ArrayList<>();
        schemaWithVersionList.add(schemaWithVersion);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(anyString());
        // get CheckCompatibility to fail
        versionInfo1 = service.addSchema("mygroup", schemaInfo).join();
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchemaVersion(anyString(), any());
        AssertExtensions.assertThrows("An exception should have been thrown", () -> service.addSchema("mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getGroupEtag(anyString());
        AssertExtensions.assertThrows("An exception should have been thrown", () -> service.addSchema("mygroup", schemaInfo).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchema(){
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(store).getSchema(anyString(), anyInt());
        SchemaInfo schemaInfo1 = service.getSchema("mygroup", 7).join();
        assertEquals(SerializationFormat.Protobuf, schemaInfo1.getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getSchema(anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchema("mygroup", 7).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchema(anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchema("mygroup",7).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingInfo(){
        EncodingId encodingId = new EncodingId(7);
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 5);
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, CodecType.GZip);
        doAnswer(x -> CompletableFuture.completedFuture(encodingInfo)).when(store).getEncodingInfo(anyString(), any());
        EncodingInfo encodingInfo1 = service.getEncodingInfo("mygroup", encodingId).join();
        assertEquals(CodecType.GZip, encodingInfo1.getCodec());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getEncodingInfo(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getEncodingInfo("mygroup", encodingId).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getEncodingInfo(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getEncodingInfo("mygroup",encodingId).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingId(){
        EncodingId encodingId = new EncodingId(7);
        Etag etag = new InMemoryGroupTable().toEtag(5);
        Either<EncodingId, Etag> encodingIdEtagEither = null;
        // Either - left
        doAnswer(x -> CompletableFuture.completedFuture(encodingIdEtagEither.left(encodingId))).when(store).getEncodingId(anyString(), any(), any());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 5);
        EncodingId encodingId1 = service.getEncodingId("mygroup", versionInfo, CodecType.GZip).join();
        assertEquals(7, encodingId1.getId());
        // createEncodingId - Right
        doAnswer(x -> CompletableFuture.completedFuture(encodingIdEtagEither.right(etag))).when(store).getEncodingId(anyString(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(encodingId)).when(store).createEncodingId(anyString(), any(), any(), any());
        encodingId1 = service.getEncodingId("mygroup", versionInfo, CodecType.GZip).join();
        assertEquals(7, encodingId1.getId());
        // CodecNotRegistered Exception
        doAnswer(x -> CompletableFuture.completedFuture(encodingIdEtagEither.right(etag))).when(store).getEncodingId(anyString(), any(), any());
        doAnswer(x -> Futures.failedFuture(new CodecNotFoundException("Codec not registered"))).when(store).createEncodingId(anyString(), any(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getEncodingId("mygroup", versionInfo, CodecType.GZip).join(), e -> e instanceof CodecNotFoundException);
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getEncodingId(anyString(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getEncodingId("mygroup", versionInfo, CodecType.GZip).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getEncodingId(anyString(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getEncodingId("mygroup",versionInfo, CodecType.GZip).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetLatestSchema(){
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 5);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(anyString());
        SchemaWithVersion schemaWithVersion1 = service.getGroupLatestSchemaVersion("mygroup", null).join();
        assertEquals(SerializationFormat.Protobuf, schemaWithVersion1.getSchema().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getLatestSchemaVersion(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupLatestSchemaVersion("mygroup", null).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getLatestSchemaVersion(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupLatestSchemaVersion("mygroup", null).join(), e -> e instanceof RuntimeException);

        // with objectType

        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(anyString(), any());
        schemaWithVersion1 = service.getGroupLatestSchemaVersion("mygroup", "objectType").join();
        assertEquals(SerializationFormat.Protobuf, schemaWithVersion1.getSchema().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getLatestSchemaVersion(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupLatestSchemaVersion("mygroup", "objectType").join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getLatestSchemaVersion(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupLatestSchemaVersion("mygroup", "objectType").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetGroupHistory(){
        // objectTypeName=null

        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        ContinuationToken continuationToken = ContinuationToken.EMPTY;
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(store).getGroupHistory(anyString());
        List<GroupHistoryRecord> groupHistoryRecords1 = service.getGroupHistory("mygroup", null).join();
        assertEquals(SerializationFormat.Avro, groupHistoryRecords1.get(0).getSchema().getSerializationFormat());
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getGroupHistory(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupHistory("mygroup",null).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupHistory(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupHistory("mygroup", null).join(), e -> e instanceof RuntimeException);

        // objectTYpeName!=null
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(store).getGroupHistoryForType(anyString(), anyString());
        groupHistoryRecords1 = service.getGroupHistory("mygroup", "myobject").join();
        assertEquals(SerializationFormat.Avro, groupHistoryRecords1.get(0).getSchema().getSerializationFormat());
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getGroupHistoryForType(anyString(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupHistory("mygroup","objectName").join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupHistoryForType(anyString(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getGroupHistory("mygroup", "objectName").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchemaVersion(){
        VersionInfo versionInfo = new VersionInfo("objectTYpe", 5, 7);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).getSchemaVersion(anyString(), any());
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo1 = service.getSchemaVersion("mygroup", schemaInfo).join();
        assertEquals(5, versionInfo1.getVersion());
        //GroupNotFoundException
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getSchemaVersion(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchemaVersion("mygroup", schemaInfo).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchemaVersion(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getSchemaVersion("mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testValidateSchema(){
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    new GroupProperties(SerializationFormat.Protobuf, SchemaValidationRules.of(Compatibility.forward()), false,
                            Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Protobuf, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(anyString());
        Boolean isValid = service.validateSchema("mygroup", schemaInfo).join();
        assertEquals(Boolean.TRUE, isValid);
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.validateSchema("mygroup", schemaInfo).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.validateSchema("mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testCanRead(){
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Protobuf, schemaData,
                        Collections.singletonMap("key", "value"));
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    new GroupProperties(SerializationFormat.Protobuf, SchemaValidationRules.of(Compatibility.forward()), false,
                            Collections.singletonMap("Encode", "false")));
        }).when(store).getGroupProperties(anyString());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(anyString());
        Boolean canRead = service.canRead("mygroup", schemaInfo).join();
        assertEquals(Boolean.TRUE, canRead);
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.canRead("mygroup", schemaInfo).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.canRead("mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testDeleteGroup(){
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).deleteGroup(anyString());
        service.deleteGroup("mygroup");
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).deleteGroup(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.deleteGroup("mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetCodecTypes(){
        List<CodecType> codecTypeList = new ArrayList<CodecType>();
        codecTypeList.add(CodecType.Snappy);
        codecTypeList.add(CodecType.GZip);
        doAnswer(x -> CompletableFuture.completedFuture(codecTypeList)).when(store).getCodecTypes(anyString());
        List<CodecType> codecTypeList1 = service.getCodecTypes("mygroup").join();
        assertEquals(CodecType.GZip, codecTypeList1.get(1));
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getCodecTypes(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getCodecTypes("mygroup").join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getCodecTypes(anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.getCodecTypes("mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddCodec(){
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).addCodec(anyString(), any());
        service.addCodec("mygroup", CodecType.GZip);
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).addCodec(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.addCodec("mygroup", CodecType.GZip).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).addCodec(anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown", () -> service.addCodec("mygroup", CodecType.GZip).join(), e -> e instanceof RuntimeException);
    }
}