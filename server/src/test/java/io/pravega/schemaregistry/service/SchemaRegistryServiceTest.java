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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import io.pravega.test.common.AssertExtensions;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class SchemaRegistryServiceTest {
    private SchemaRegistryService service;
    private ScheduledExecutorService executor;
    private SchemaStore store;

    private String avroUnionSchema = "[\n" +
            "  {\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Address\",\n" +
            "    \"fields\": [\n" +
            "      {\n" +
            "        \"name\": \"streetaddress\",\n" +
            "        \"type\": \"string\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"city\",\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  {\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Person\",\n" +
            "    \"fields\": [\n" +
            "      {\n" +
            "        \"name\": \"firstname\",\n" +
            "        \"type\": \"string\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"lastname\",\n" +
            "        \"type\": \"string\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"address\",\n" +
            "        \"type\": \"Address\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "]"; 
    
    private String avroComplexObject = "{\n" +
            "    \"name\": \"Person\",\n" +
            "    \"type\": \"record\",\n" +
            "    \"fields\": [\n" +
            "        {\"name\": \"firstname\", \"type\": \"string\"},\n" +
            "        {\"name\": \"lastname\", \"type\": \"string\"},\n" +
            "        {\n" +
            "            \"name\": \"address\",\n" +
            "            \"type\": {\n" +
            "                        \"type\" : \"record\",\n" +
            "                        \"name\" : \"Address\",\n" +
            "                        \"fields\" : [\n" +
            "                            {\"name\": \"streetaddress\", \"type\": \"string\"},\n" +
            "                            {\"name\": \"city\", \"type\": \"string\"}\n" +
            "                        ]\n" +
            "                    }\n" +
            "        }\n" +
            "    ]\n" +
            "}";
    @Before
    public void setUp() {

        executor = Executors.newScheduledThreadPool(5);
        store = mock(SchemaStore.class);
        service = new SchemaRegistryService(store, executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testListGroups() {
        ArrayList<String> groups = Lists.newArrayList("grp1", "grp2");
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new ResultPage<>(groups, null));
        }).when(store).listGroups(any(), any(), anyInt());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SerializationFormat.Avro,
                    Compatibility.backward(), false));
        }).when(store).getGroupProperties(any(), eq("grp1"));

        doAnswer(x -> {
            return Futures.failedFuture(
                    StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "group prop not found"));
        }).when(store).getGroupProperties(any(), eq("grp2"));

        ResultPage<Map.Entry<String, GroupProperties>, ContinuationToken> result = service.listGroups(null, null,
                100).join();
        assertEquals(result.getList().size(), 1);
    }

    @Test
    public void testCreateGroup() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Avro).compatibility(
                Compatibility.forward()).build();
        doAnswer(x -> {
            return CompletableFuture.completedFuture(true);
        }).when(store).createGroup(any(), anyString(), any());
        Boolean ans = service.createGroup(null, "mygroup", groupProperties).join();
        assertEquals(true, ans);
        // already exists
        doAnswer(x -> {
            return CompletableFuture.completedFuture(false);
        }).when(store).createGroup(any(), anyString(), any());
        ans = service.createGroup(null, "mygroup", groupProperties).join();
        assertEquals(Boolean.FALSE, ans);
    }

    @Test
    public void testGetGroupProperties() {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                            ImmutableMap.<String, String>builder().build()).serializationFormat(
                            SerializationFormat.Avro).compatibility(
                            Compatibility.forward()).build());
        }).when(store).getGroupProperties(any(), anyString());
        GroupProperties groupProperties = service.getGroupProperties(null, "mygroup").join();
        assertEquals(SerializationFormat.Avro, groupProperties.getSerializationFormat());
        assertEquals(Compatibility.forward(), groupProperties.getCompatibility());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.getGroupProperties(null, "mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testUpdateCompatibility() throws ExecutionException, InterruptedException {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(null);
        }).when(store).updateCompatibility(any(), anyString(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5))).when(store).getGroupEtag(
                any(), anyString());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                            ImmutableMap.<String, String>builder().build()).serializationFormat(
                            SerializationFormat.Avro).compatibility(
                            Compatibility.forward()).build());
        }).when(store).getGroupProperties(any(), anyString());
        service.updateCompatibility(null, "mygroup", Compatibility.backward(),
                Compatibility.forward());
        service.updateCompatibility(null, "mygroup", Compatibility.backward(), null).join();
        // PreconditionFailed Exception
        doAnswer(x -> CompletableFuture.completedFuture(
                GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                        ImmutableMap.<String, String>builder().build()).serializationFormat(
                        SerializationFormat.Avro).compatibility(
                        Compatibility.backward()).build())).when(store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateCompatibility(null, "mygroup", Compatibility.forward(),
                        Compatibility.forward()).join(),
                e -> e instanceof PreconditionFailedException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).updateCompatibility(any(), anyString(),
                any(), any());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.updateCompatibility(null, "mygroup", Compatibility.forward(),
                        Compatibility.backward()).join(), e -> Exceptions.unwrap(e) instanceof RuntimeException);
    }

    @Test
    public void testGetSchemas() {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        List<SchemaWithVersion> schemaWithVersions = new ArrayList<>();
        schemaWithVersions.add(schemaWithVersion);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersions)).when(store).listLatestSchemas(any(),
                anyString());
        List<SchemaWithVersion> schemaWithVersionList = service.getSchemas(null, "mygroup", null).join();
        assertEquals(1, schemaWithVersionList.size());
        // Runtime Exception
        doAnswer(x -> new RuntimeException()).when(store).listLatestSchemas(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemas(null, "mygroup", null),
                e -> e instanceof RuntimeException);
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).listLatestSchemas(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemas(null, "mygroup", null).join(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testAddSchema() {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5));
        }).when(store).getGroupEtag(any(), anyString());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                            ImmutableMap.<String, String>builder().build()).serializationFormat(
                            SerializationFormat.custom("custom1")).compatibility(
                            Compatibility.forward()).build());
        }).when(store).getGroupProperties(any(), anyString());
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("type", SerializationFormat.custom("custom1"),
                ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).addSchema(any(), anyString(), any(), any(),
                any(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).getSchemaVersion(any(), anyString(),
                any(), any());
        VersionInfo versionInfo1 = service.addSchema(null, "mygroup", schemaInfo).join();
        assertEquals(7, versionInfo1.getId());
        // SerializationFormatMismatch Exception
        doAnswer(x -> CompletableFuture.completedFuture(
                GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                        ImmutableMap.<String, String>builder().build()).serializationFormat(
                        SerializationFormat.Avro).compatibility(
                        Compatibility.forward()).build())).when(store).getGroupProperties(
                any(), anyString());
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getSchemaVersion(any(), anyString(), any(), any());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.addSchema(null, "mygroup", schemaInfo).join(),
                e -> e instanceof SerializationFormatMismatchException);

        // IncompatibleSchema Exception
        doAnswer(x -> CompletableFuture.completedFuture(
                GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                        ImmutableMap.<String, String>builder().build()).serializationFormat(
                        SerializationFormat.custom("custom1")).compatibility(
                        Compatibility.allowAny()).build())).when(store).getGroupProperties(
                any(), anyString());
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getSchemaVersion(any(), anyString(), any(), any());
        schemaData = new byte[1];
        SchemaInfo schemaInfo1 = new SchemaInfo("type1", SerializationFormat.custom("custom1"),
                ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo1, versionInfo);
        List<SchemaWithVersion> schemaWithVersionList = new ArrayList<>();
        schemaWithVersionList.add(schemaWithVersion);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersionList)).when(store).listLatestSchemas(
                any(), anyString());
        // CheckCompatibility will fail due to differing types. allowMultipleTypes is false.
        AssertExtensions.assertThrows("An exception should have been thrown", () -> service.addSchema(null, "mygroup", schemaInfo).join(), e -> e instanceof IncompatibleSchemaException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchemaVersion(any(), anyString(), any(),
                any());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.addSchema(null, "mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getGroupEtag(any(), anyString());
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> service.addSchema(null, "mygroup", schemaInfo).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchema() {
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(store).getSchema(any(), anyString(),
                anyInt());
        SchemaInfo schemaInfo1 = service.getSchema(null, "mygroup", 7).join();
        assertEquals(SerializationFormat.Protobuf, schemaInfo1.getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getSchema(any(), anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchema(null, "mygroup", 7).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchema(any(), anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchema(null, "mygroup", 7).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingInfo() {
        EncodingId encodingId = new EncodingId(7);
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SerializationFormat.Protobuf, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 5);
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, new CodecType("gzip"));
        doAnswer(x -> CompletableFuture.completedFuture(encodingInfo)).when(store).getEncodingInfo(any(), anyString(),
                any());
        EncodingInfo encodingInfo1 = service.getEncodingInfo(null, "mygroup", encodingId).join();
        assertEquals(new CodecType("gzip"), encodingInfo1.getCodecType());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getEncodingInfo(any(), anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getEncodingInfo(null, "mygroup", encodingId).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getEncodingInfo(any(), anyString(),
                any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getEncodingInfo(null, "mygroup", encodingId).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingId() {
        EncodingId encodingId = new EncodingId(7);
        Etag etag = new InMemoryGroupTable().toEtag(5);
        Either<EncodingId, Etag> encodingIdEtagEither = null;
        // Either - left
        doAnswer(x -> CompletableFuture.completedFuture(Either.left(encodingId))).when(
                store).getEncodingId(any(), anyString(), any(), any());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 5);
        EncodingId encodingId1 = service.getEncodingId(null, "mygroup", versionInfo, "gzip").join();
        assertEquals(7, encodingId1.getId());
        // createEncodingId - Right
        doAnswer(x -> CompletableFuture.completedFuture(Either.right(etag))).when(store).getEncodingId(
                any(), anyString(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(encodingId)).when(store).createEncodingId(any(), anyString(),
                any(),
                any(), any());
        encodingId1 = service.getEncodingId(null, "mygroup", versionInfo, "gzip").join();
        assertEquals(7, encodingId1.getId());
        // CodecNotRegistered Exception
        doAnswer(x -> Futures.failedFuture(new CodecTypeNotRegisteredException("Codec not registered"))).when(
                store).createEncodingId(any(), anyString(), any(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getEncodingId(null, "mygroup", versionInfo, "gzip").join(),
                e -> e instanceof CodecTypeNotRegisteredException);
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                store).getEncodingId(any(), anyString(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getEncodingId(null, "mygroup", versionInfo, "gzip").join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getEncodingId(any(), anyString(), any(),
                any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getEncodingId(null, "mygroup", versionInfo, "gzip").join(),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetGroupHistory() {
        // objectTypeName=null
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                Compatibility.allowAny(), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        ContinuationToken continuationToken = ContinuationToken.EMPTY;
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(store).getGroupHistory(any(),
                anyString());
        List<GroupHistoryRecord> groupHistoryRecords1 = service.getGroupHistory(null, "mygroup", null).join();
        assertEquals(SerializationFormat.Avro, groupHistoryRecords1.get(0).getSchemaInfo().getSerializationFormat());
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getGroupHistory(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getGroupHistory(null, "mygroup", null).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupHistory(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getGroupHistory(null, "mygroup", null).join(), e -> e instanceof RuntimeException);

        // objectTYpeName!=null
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(store).getGroupHistoryForType(
                any(), anyString(), anyString());
        groupHistoryRecords1 = service.getGroupHistory(null, "mygroup", "myobject").join();
        assertEquals(SerializationFormat.Avro, groupHistoryRecords1.get(0).getSchemaInfo().getSerializationFormat());
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getGroupHistoryForType(any(), anyString(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getGroupHistory(null, "mygroup", "objectName").join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupHistoryForType(any(),
                anyString(),
                anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getGroupHistory(null, "mygroup", "objectName").join(),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchemaVersion() {
        VersionInfo versionInfo = new VersionInfo("objectTYpe", 5, 7);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).getSchemaVersion(any(), anyString(), any(),
                any());
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo1 = service.getSchemaVersion(null, "mygroup", schemaInfo).join();
        assertEquals(5, versionInfo1.getVersion());
        //GroupNotFoundException
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getSchemaVersion(any(), anyString(), any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemaVersion(null, "mygroup", schemaInfo).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchemaVersion(any(), anyString(), any(),
                any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemaVersion(null, "mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testValidateSchema() {
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                            ImmutableMap.<String, String>builder().build()).serializationFormat(
                            SerializationFormat.Custom).compatibility(Compatibility.forward()).build());
        }).when(store).getGroupProperties(any(), anyString());
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.custom("custom1"), ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(
                any(), anyString());
        Boolean isValid = service.validateSchema(null, "mygroup", schemaInfo, Compatibility.forward()).join();
        assertEquals(Boolean.TRUE, isValid);
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.validateSchema(null, "mygroup", schemaInfo, Compatibility.forward()).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.validateSchema(null, "mygroup", schemaInfo, Compatibility.forward()).join(),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testCanRead() {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "packageName.schemaName", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(
                    GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                            ImmutableMap.<String, String>builder().build()).serializationFormat(
                            SerializationFormat.Custom).compatibility(Compatibility.forward()).build());
        }).when(store).getGroupProperties(any(), anyString());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(store).getLatestSchemaVersion(
                any(), anyString());
        Boolean canRead = service.canRead(null, "mygroup", schemaInfo).join();
        assertEquals(Boolean.TRUE, canRead);
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.canRead(null, "mygroup", schemaInfo).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupProperties(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.canRead(null, "mygroup", schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testDeleteGroup() {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).deleteGroup(any(), anyString());
        service.deleteGroup(null, "mygroup");
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).deleteGroup(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.deleteGroup(null, "mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetCodecTypes() {
        List<String> codecTypes = new ArrayList<>();
        codecTypes.add("snappy");
        codecTypes.add("gzip");
        doAnswer(x -> CompletableFuture.completedFuture(codecTypes)).when(store).listCodecTypes(any(), anyString());
        List<CodecType> codecTypeList1 = service.getCodecTypes(null, "mygroup").join();
        assertEquals("gzip", codecTypeList1.get(1));
        // GroupNotFound
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).listCodecTypes(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getCodecTypes(null, "mygroup").join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).listCodecTypes(any(), anyString());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getCodecTypes(null, "mygroup").join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddCodecType() {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).addCodecType(any(), anyString(), any());
        service.addCodecType(null, "mygroup", new CodecType("gzip"));
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).addCodecType(
                any(), anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.addCodecType(null, "mygroup", new CodecType("gzip")).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).addCodecType(any(), anyString(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.addCodecType(null, "mygroup", new CodecType("gzip")).join(),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchemaReferences() {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.custom("custom1"), ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        String groupName = "mygroup";
        List<String> groupNameList = new ArrayList<>();
        groupNameList.add(groupName);
        doAnswer(x -> CompletableFuture.completedFuture(groupNameList)).when(store).getGroupsUsing(any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(store).getSchemaVersion(any(), anyString(), any(),
                any());
        Map<String, VersionInfo> map = service.getSchemaReferences(null, schemaInfo).join();
        assertTrue(map.get(groupName).equals(versionInfo));
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).getGroupsUsing(
                any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemaReferences(null, schemaInfo).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getGroupsUsing(any(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchemaReferences(null, schemaInfo).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchemaWithVersionAndType() {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.custom("custom1"), ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(store).getSchema(any(), anyString(),
                anyString(), anyInt());
        SchemaInfo schemaInfo1 = service.getSchema(null, groupName, "schemaName", versionInfo.getVersion()).join();
        assertTrue(schemaInfo.equals(schemaInfo1));
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(store).getSchema(
                any(), anyString(), anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchema(null, groupName, "schemaName", versionInfo.getVersion()).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).getSchema(any(), anyString(),
                anyString(), anyInt());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.getSchema(null, groupName, "schemaName", versionInfo.getVersion()).join(),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testDeleteSchema() {
        int ordinal = 5;
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5))).when(store).getGroupEtag(
                any(), anyString());
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).deleteSchema(any(), anyString(), anyInt(),
                any());
        service.deleteSchema(null, groupName, ordinal).join();
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).deleteSchema(any(), anyString(), anyInt(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.deleteSchema(null, groupName, ordinal).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).deleteSchema(any(), anyString(),
                anyInt(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.deleteSchema(null, groupName, ordinal).join(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testDeleteUsingTypeAndVersion() {
        int version = 5;
        String groupName = "mygroup";
        String schemaName = "myschema";
        doAnswer(x -> CompletableFuture.completedFuture(new InMemoryGroupTable().toEtag(5))).when(store).getGroupEtag(
                any(), anyString());
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(store).deleteSchema(any(), anyString(), anyString(),
                anyInt(), any());
        service.deleteSchema(null, groupName, schemaName, version).join();
        //GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(
                StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "Group NotFound"))).when(
                store).deleteSchema(any(), anyString(), anyString(), anyInt(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.deleteSchema(null, groupName, schemaName, version).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(store).deleteSchema(any(), anyString(),
                anyString(), anyInt(), any());
        AssertExtensions.assertThrows("An Exception should have been thrown",
                () -> service.deleteSchema(null, groupName, schemaName, version).join(),
                e -> e instanceof RuntimeException);
    }
    
    @Test
    public void testSchemaNormalization() {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "g";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(false).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Json)
                               .compatibility(Compatibility.allowAny()).build()).join();
        
        String jsonSchemaString = "{" +
                "\"title\": \"Person\", " +
                "\"type\": \"object\", " +
                "\"properties\": { " +
                "\"name\": {" +
                "\"type\": \"string\"" +
                "}," +
                "\"age\": {" +
                "\"type\": \"integer\", \"minimum\": 0" +
                "}" +
                "}" +
                "}";
        String jsonSchemaString2 = "{" +
                "\"title\": \"Person\", " +
                "\"type\": \"object\", " +
                "\"properties\": { " +
                "\"age\": {" +
                "\"type\": \"integer\", \"minimum\": 0" +
                "}," +
                "\"name\": {" +
                "\"type\": \"string\"" +
                "}" +
                "}" +
                "}";
        SchemaInfo original = SchemaInfo.builder().type("person").serializationFormat(SerializationFormat.Json)
                                      .schemaData(ByteBuffer.wrap(jsonSchemaString.getBytes(Charsets.UTF_8)))
                                      .properties(ImmutableMap.of()).build();
        VersionInfo v = service.addSchema(namespace, group, original).join();
        SchemaInfo schema = service.getSchema(namespace, group, v.getId()).join();
        assertEquals(schema, original);

        // check with different order
        SchemaInfo secondOrder = SchemaInfo.builder().type("person").serializationFormat(SerializationFormat.Json)
                                          .schemaData(ByteBuffer.wrap(jsonSchemaString2.getBytes(Charsets.UTF_8)))
                                          .properties(ImmutableMap.of()).build();
        VersionInfo v2 = service.addSchema(namespace, group, secondOrder).join();
        // add should have been idempotent
        assertEquals(v2, v);

        schema = service.getSchema(namespace, group, v.getId()).join();
        assertNotEquals(schema, secondOrder);
    }
    
    @Test
    public void testSchemaInfoWithoutTypeAvro() {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Avro)
                               .compatibility(Compatibility.allowAny()).build()).join();
        
        SchemaInfo withoutType = SchemaInfo.builder().type("").serializationFormat(SerializationFormat.Avro)
                                      .schemaData(ByteBuffer.wrap(avroComplexObject.getBytes(Charsets.UTF_8)))
                                      .properties(ImmutableMap.of()).build();
        VersionInfo v = service.addSchema(namespace, group, withoutType).join();
        assertEquals(v.getType(), "Person");
        SchemaInfo schema = service.getSchema(namespace, group, v.getId()).join();
        assertEquals(schema.getType(), "Person");
        service.deleteSchema(namespace, group, v.getId());
    }
    
    @Test
    public void testSchemaInfoWithoutTypeUnionAvro() {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Avro)
                               .compatibility(Compatibility.allowAny()).build()).join();
        
        SchemaInfo withPerson = SchemaInfo.builder().type("").serializationFormat(SerializationFormat.Avro)
                                        .schemaData(ByteBuffer.wrap(avroUnionSchema.getBytes(Charsets.UTF_8)))
                                        .properties(ImmutableMap.of()).build();
        VersionInfo v2 = service.addSchema(namespace, group, withPerson).join();
        SchemaInfo schema = service.getSchema(namespace, group, v2.getId()).join();
        assertEquals(schema.getType(), new Schema.Parser().parse(avroUnionSchema).getName());
    }
    
    @Test
    public void testSchemaInfoWithTypeUnionAvro() {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Avro)
                               .compatibility(Compatibility.allowAny()).build()).join();
        
        SchemaInfo withAddress = SchemaInfo.builder().type("Address").serializationFormat(SerializationFormat.Avro)
                                        .schemaData(ByteBuffer.wrap(avroUnionSchema.getBytes(Charsets.UTF_8)))
                                        .properties(ImmutableMap.of()).build();
        VersionInfo v3 = service.addSchema(namespace, group, withAddress).join();
        assertEquals(v3.getType(), "Address");
        SchemaInfo schema = service.getSchema(namespace, group, v3.getId()).join();
        assertEquals(schema.getType(), "Address");
    }
    
    @Test
    public void testSchemaInfoIllegalTypeAvro() {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Avro)
                               .compatibility(Compatibility.allowAny()).build()).join();
        
        SchemaInfo incorrectnamespace = SchemaInfo.builder().type("a.Address").serializationFormat(SerializationFormat.Avro)
                                        .schemaData(ByteBuffer.wrap(avroUnionSchema.getBytes(Charsets.UTF_8)))
                                        .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal namespace", () -> service.addSchema(namespace, group, incorrectnamespace), 
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        SchemaInfo incorrectname = SchemaInfo.builder().type("a").serializationFormat(SerializationFormat.Avro)
                                        .schemaData(ByteBuffer.wrap(avroUnionSchema.getBytes(Charsets.UTF_8)))
                                        .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal name", () -> service.addSchema(namespace, group, incorrectname), 
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
    }
    
    @Test
    public void testSchemaInfoWithoutTypeProto() throws IOException {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Protobuf)
                               .compatibility(Compatibility.allowAny()).build()).join();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        SchemaInfo incorrectpackage1 = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.")
                                                .serializationFormat(SerializationFormat.Protobuf)
                                                .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                                .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal package", () -> service.addSchema(namespace, group, incorrectpackage1),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        SchemaInfo withoutType = SchemaInfo.builder().type("").serializationFormat(SerializationFormat.Protobuf)
                                      .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                      .properties(ImmutableMap.of()).build();
        VersionInfo v = service.addSchema(namespace, group, withoutType).join();
        assertEquals(v.getType(), "io.pravega.schemaregistry.testobjs.generated.InternalMessage");
        SchemaInfo schema = service.getSchema(namespace, group, v.getId()).join();
        assertEquals(schema.getType(), "io.pravega.schemaregistry.testobjs.generated.InternalMessage");
    }
    
    @Test
    public void testSchemaInfoWithTypeWithPackageProto() throws IOException {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Protobuf)
                               .compatibility(Compatibility.allowAny()).build()).join();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        SchemaInfo incorrectpackage1 = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.")
                                                .serializationFormat(SerializationFormat.Protobuf)
                                                .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                                .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal package", () -> service.addSchema(namespace, group, incorrectpackage1),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        SchemaInfo withPackage = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.Message2")
                                           .serializationFormat(SerializationFormat.Protobuf)
                                      .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                      .properties(ImmutableMap.of()).build();
        VersionInfo v2 = service.addSchema(namespace, group, withPackage).join();
        assertEquals(v2.getType(), "io.pravega.schemaregistry.testobjs.generated.Message2");
        SchemaInfo schema = service.getSchema(namespace, group, v2.getId()).join();
        assertEquals(schema.getType(), "io.pravega.schemaregistry.testobjs.generated.Message2");
    }
    
    @Test
    public void testSchemaInfoWithAndWitTypeNoPackageProto() throws IOException {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Protobuf)
                               .compatibility(Compatibility.allowAny()).build()).join();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        SchemaInfo incorrectpackage1 = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.")
                                                .serializationFormat(SerializationFormat.Protobuf)
                                                .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                                .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal package", () -> service.addSchema(namespace, group, incorrectpackage1),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        SchemaInfo withoutPackage = SchemaInfo.builder().type("Message1").serializationFormat(SerializationFormat.Protobuf)
                                        .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                        .properties(ImmutableMap.of()).build();
        VersionInfo v3 = service.addSchema(namespace, group, withoutPackage).join();
        assertEquals(v3.getType(), "io.pravega.schemaregistry.testobjs.generated.Message1");
        SchemaInfo schema = service.getSchema(namespace, group, v3.getId()).join();
        assertEquals(schema.getType(), "io.pravega.schemaregistry.testobjs.generated.Message1");
    }
    
    @Test
    public void testSchemaInfoWithInvalidTypeProto() throws IOException {
        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);
        String namespace = "n";
        String group = "gav";
        service.createGroup(namespace, group,
                GroupProperties.builder().allowMultipleTypes(true).properties(ImmutableMap.<String, String>builder().build())
                               .serializationFormat(SerializationFormat.Protobuf)
                               .compatibility(Compatibility.allowAny()).build()).join();
        Path path = Paths.get("src/test/resources/proto/protobufTest.pb");
        byte[] schemaBytes = Files.readAllBytes(path);
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaBytes);

        SchemaInfo incorrectpackage1 = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.")
                                                .serializationFormat(SerializationFormat.Protobuf)
                                                .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                                .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal package", () -> service.addSchema(namespace, group, incorrectpackage1),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        SchemaInfo incorrectpackage = SchemaInfo.builder().type("io.pravega.schemaregistry.testobjs.generated.")
                                                .serializationFormat(SerializationFormat.Protobuf)
                                        .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                        .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal package", () -> service.addSchema(namespace, group, incorrectpackage), 
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        SchemaInfo notpresentName = SchemaInfo.builder().type("notpresentName").serializationFormat(SerializationFormat.Protobuf)
                                              .schemaData(ByteBuffer.wrap(descriptorSet.toByteArray()))
                                              .properties(ImmutableMap.of()).build();
        AssertExtensions.assertThrows("Illegal namespace", () -> service.addSchema(namespace, group, notpresentName),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
    }
}