/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GroupTest {
    String groupId;
    InMemoryGroupTable integerGroupTable;
    Group<Integer> integerGroup;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(5);
        groupId = "mygroup";
        integerGroupTable = new InMemoryGroupTable();
        integerGroup = new Group<Integer>(integerGroupTable, executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        List<TableRecords.TableValue> recordList = integerGroupTable.getTable().entrySet().stream().map(
                x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(Boolean.TRUE, recordList.contains(
                new TableRecords.ValidationRecord(Compatibility.backward())));
    }

    @Test
    public void testGetCurrentEtag() {
        // null case
        Etag eTag = integerGroup.getCurrentEtag().join();
        assertEquals(null, integerGroupTable.fromEtag(eTag));
        assertTrue(integerGroupTable.getTable().isEmpty());
        // non null case
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        List<Integer> integerList = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getValue().getValue().equals(
                        new TableRecords.ValidationRecord(Compatibility.backward()))).map(
                x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(1, integerList.size());
        eTag = integerGroup.getCurrentEtag().join();
        assertEquals(integerList.get(0), integerGroupTable.fromEtag(eTag));
    }

    @Test
    public void testGetTypes() {
        // null case
        List<SchemaWithVersion> schemaWithVersionList = integerGroup.getLatestSchemas().join();
        assertTrue(integerGroupTable.getTable().isEmpty());
        assertEquals(Collections.emptyList(), schemaWithVersionList);
        // non-null case
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchema(schemaInfo1, groupProperties, eTag).join();
        schemaWithVersionList = integerGroup.getLatestSchemas().join();
        assertEquals(2, schemaWithVersionList.size());

        assertEquals("anygroup", schemaWithVersionList.get(0).getSchemaInfo().getType());
        assertEquals("anygroup1", schemaWithVersionList.get(1).getSchemaInfo().getType());

    }

    @Test
    public void testAddSchema() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        String mygroup = "mygroup";
        SchemaInfo schemaInfo = new SchemaInfo(mygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        List<TableRecords.TableValue> tableValueListEtag = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.Etag).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListEtag.isEmpty());

        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertTrue(tableValueListVersionInfo.get(0) instanceof TableRecords.SchemaRecord);
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(0);
        assertEquals(mygroup, schemaRecord.getSchemaInfo().getType());
        assertEquals(0, schemaRecord.getId());

        List<TableRecords.TableValue> tableValueListSchemaInfo = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaFingerprintKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListSchemaInfo.isEmpty());
        assertTrue(tableValueListSchemaInfo.get(0) instanceof TableRecords.SchemaVersionList);
        TableRecords.SchemaVersionList schemaVersionValue =
                (TableRecords.SchemaVersionList) tableValueListSchemaInfo.get(
                        0);
        assertEquals(0, schemaVersionValue.getVersions().get(0).getVersion());

        List<TableRecords.TableValue> tableValueListLatestSchema = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.LatestSchemasKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListLatestSchema.isEmpty());
        assertTrue(tableValueListLatestSchema.get(0) instanceof TableRecords.LatestSchemasValue);
        TableRecords.LatestSchemasValue latestSchemaVersionValue =
                (TableRecords.LatestSchemasValue) tableValueListLatestSchema.get(
                        0);
        assertEquals(0, latestSchemaVersionValue.getTypes().get(mygroup).getLatestVersion());
        assertTrue(latestSchemaVersionValue.getTypes().get(mygroup).getDeletedVersions().isEmpty());
    }

    @Test
    public void testGetSchemas() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchema(schemaInfo1, groupProperties, eTag).join();
        List<SchemaWithVersion> schemaWithVersionListWithToken = integerGroup.getSchemas().join();
        assertEquals(2, schemaWithVersionListWithToken.size());
        assertEquals(SerializationFormat.Custom,
                schemaWithVersionListWithToken.get(0).getSchemaInfo().getSerializationFormat());
        // with ObjectTypeName
        schemaWithVersionListWithToken = integerGroup.getSchemas("anygroup").join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        schemaWithVersionListWithToken = integerGroup.getSchemas("anygroup1").join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(2, tableValueListVersionInfo.size());
        assertFalse(tableValueListVersionInfo.isEmpty());
        List<Integer> version = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(2, version.size());
        List<TableRecords.TableKey> tableKeyObjectTypes = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.LatestSchemasKey).map(
                x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, tableKeyObjectTypes.size());
        TableRecords.LatestSchemasValue objectTypesListValue = integerGroupTable.getEntry(tableKeyObjectTypes.get(0),
                TableRecords.LatestSchemasValue.class).join();
        assertEquals(2, objectTypesListValue.getTypes().size());
    }

    @Test
    public void testGetVersion() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchema(schemaInfo1, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo1).join();
        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertEquals(2, tableValueListVersionInfo.size());
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(0);
        TableRecords.SchemaRecord schemaRecord1 = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(1);
        assertEquals(schemaRecord.getVersion(), versionInfo.getVersion());
        assertEquals(schemaRecord1.getVersion(), versionInfo1.getVersion());
    }

    @Test
    public void testAddCodec() {
        integerGroup.addCodecType(new CodecType("gzip")).join();
        List<TableRecords.TableKey> codecsListKey = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecTypesKey).map(x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecTypesListValue codecsListValue = integerGroupTable.getEntry(codecsListKey.get(0),
                TableRecords.CodecTypesListValue.class).join();
        assertEquals("gzip", codecsListValue.getCodecTypes().get(0));
    }

    @Test
    public void testCreateEncodingId() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodecType(new CodecType("gzip")).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        integerGroup.addCodecType(new CodecType("snappy")).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        versionInfo = integerGroup.getVersion(schemaInfo).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId1 = integerGroup.createEncodingId(versionInfo, "snappy", eTag).join();
        List<TableRecords.TableValue> encodingInfoRecordList = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        List<TableRecords.TableKey> encodingIdObtained = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getKey()).collect(
                Collectors.toList());
        System.out.println(encodingInfoRecordList);
        assertEquals(2, encodingInfoRecordList.size());
        assertEquals(new TableRecords.EncodingIdRecord(encodingId), encodingIdObtained.get(0));
        assertEquals(new TableRecords.EncodingIdRecord(encodingId1), encodingIdObtained.get(1));
    }

    @Test
    public void testGetEncodingInfo() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodecType(new CodecType("gzip")).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        EncodingInfo encodingInfo = integerGroup.getEncodingInfo(encodingId).join();
        assertEquals(new CodecType("gzip"), encodingInfo.getCodecType());
        assertEquals(versionInfo, encodingInfo.getVersionInfo());
        assertEquals(SerializationFormat.Custom, encodingInfo.getSchemaInfo().getSerializationFormat());
    }

    @Test
    public void testGetLatestSchemaVersion() {
        // null case
        SchemaWithVersion schemaWithVersion = integerGroup.getLatestSchemaVersion().join();
        assertNull(schemaWithVersion);
        // non-null case
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        schemaWithVersion = integerGroup.getLatestSchemaVersion().join();
        assertEquals(versionInfo, schemaWithVersion.getVersionInfo());
        assertEquals("anygroup", schemaWithVersion.getSchemaInfo().getType());

        // objectTypeName
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo1, groupProperties, eTag).join();
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo1).join();
        // null
        schemaWithVersion = integerGroup.getLatestSchemaVersion("anygroup2").join();
        assertNull(schemaWithVersion);
        // non-null
        schemaWithVersion = integerGroup.getLatestSchemaVersion("anygroup1").join();
        assertEquals(versionInfo1, schemaWithVersion.getVersionInfo());
        schemaWithVersion = integerGroup.getLatestSchemaVersion("anygroup").join();
        assertTrue(Arrays.equals(schemaInfo.getSchemaData().array(),
                schemaWithVersion.getSchemaInfo().getSchemaData().array()));
    }

    @Test
    public void testGetCodecTypes() {
        // null
        List<CodecType> codecTypeList = integerGroup.getCodecTypes().join();
        assertEquals(0, codecTypeList.size());
        assertTrue(codecTypeList.isEmpty());
        // non-null
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addCodecType(new CodecType("snappy")).join();
        integerGroup.addCodecType(new CodecType("gzip")).join();
        codecTypeList = integerGroup.getCodecTypes().join();
        List<TableRecords.TableKey> codecsListKey = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecTypesKey).map(x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecTypesListValue codecsListValue = integerGroupTable.getEntry(codecsListKey.get(0),
                TableRecords.CodecTypesListValue.class).join();
        assertEquals(2, codecsListValue.getCodecTypes().size());
        assertEquals("snappy", codecsListValue.getCodecTypes().get(0));
        assertEquals(2, codecTypeList.size());
        System.out.println(codecsListValue);
        System.out.println(codecTypeList);
        assertEquals(codecsListValue.getCodecTypes().get(0), codecTypeList.get(0).getName());
        assertEquals(codecsListValue.getCodecTypes().get(1), codecTypeList.get(1).getName());
    }

    @Test
    public void testGetHistory() {
        integerGroup.create(SerializationFormat.Avro, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(), Boolean.TRUE,
                ImmutableMap.of());
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        List<GroupHistoryRecord> groupHistoryRecords = integerGroup.getHistory().join();
        assertEquals(2, groupHistoryRecords.size());
        assertEquals("anygroup", groupHistoryRecords.get(0).getSchemaInfo().getType());
        assertEquals("anygroup1", groupHistoryRecords.get(1).getSchemaInfo().getType());
        // objectType
        byte[] schemaData1 = new byte[10];
        schemaInfo = new SchemaInfo("anygroup1", SerializationFormat.Avro, ByteBuffer.wrap(schemaData1),
                ImmutableMap.of());
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        groupHistoryRecords = integerGroup.getHistory("anygroup1").join();
        assertEquals(2, groupHistoryRecords.size());
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData).array(),
                groupHistoryRecords.get(0).getSchemaInfo().getSchemaData().array()));
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData1).array(),
                groupHistoryRecords.get(1).getSchemaInfo().getSchemaData().array()));
    }

    @Test
    public void testUpdateValidationPolicy() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        integerGroup.updateValidationPolicy(Compatibility.forward(), eTag).join();
        List<TableRecords.TableValue> validationRecord = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.ValidationPolicyKey).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(1, validationRecord.size());
        TableRecords.ValidationRecord validationRecord1 = (TableRecords.ValidationRecord) validationRecord.get(0);
        assertEquals(Compatibility.forward(), validationRecord1.getCompatibility());
        // when unchanged
        eTag = integerGroup.getCurrentEtag().join();
        assertNull(integerGroup.updateValidationPolicy(Compatibility.forward(), eTag).join());
    }

    @Test
    public void testGetGroupProperties() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        GroupProperties groupProperties = integerGroup.getGroupProperties().join();
        assertEquals(SerializationFormat.Custom, groupProperties.getSerializationFormat());
        assertEquals(Compatibility.backward(), groupProperties.getCompatibility());
        assertTrue(groupProperties.isAllowMultipleTypes());
        assertEquals(ImmutableMap.of(), groupProperties.getProperties());
    }

    @Test
    public void testGetEncodingId() {
        //null
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodecType(new CodecType("gzip")).join();
        Either<EncodingId, Etag> idEtagEither = integerGroup.getEncodingId(versionInfo, "gzip").join();
        assertTrue(idEtagEither.isRight());
        //non-null
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        integerGroup.addCodecType(new CodecType("snappy")).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag);
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId1 = integerGroup.createEncodingId(versionInfo1, "snappy", eTag).join();
        idEtagEither = integerGroup.getEncodingId(versionInfo, "gzip").join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId, idEtagEither.getLeft());
        idEtagEither = integerGroup.getEncodingId(versionInfo1, "snappy").join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId1, idEtagEither.getLeft());
    }

    @Test
    public void testDeleteSchema() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag);
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.deleteSchema(versionInfo.getId(), eTag).join();
        List<TableRecords.TableValue> deletedOrdinalList = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionDeletedRecord).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(1, deletedOrdinalList.size());
        TableRecords.VersionDeletedRecord versionDeletedRecord =
                (TableRecords.VersionDeletedRecord) deletedOrdinalList.get(
                        0);
        assertEquals(versionInfo.getId(), versionDeletedRecord.getId());
        // already deleted
        eTag = integerGroup.getCurrentEtag().join();
        assertNull(integerGroup.deleteSchema(versionInfo.getId(), eTag).join());
    }

    @Test
    public void testGetSchema() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        SchemaInfo schemaInfo1 = integerGroup.getSchema(versionInfo.getId()).join();
        assertEquals(schemaInfo, schemaInfo1);
        List<TableRecords.TableValue> schemaRecordValues = integerGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey().equals(new TableRecords.SchemaIdKey(versionInfo.getId()))).map(
                x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(1, schemaRecordValues.size());
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) schemaRecordValues.get(0);
        assertEquals(schemaRecord.getSchemaInfo(), schemaInfo);
        // trying with an incorrect value of ordinal
        AssertExtensions.assertThrows("An exception should have been thrown", () -> integerGroup.getSchema(100).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchemaUsingTypeAndVersion() {
        integerGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        SchemaInfo schemaInfo1 = integerGroup.getSchema(versionInfo.getType(), versionInfo.getVersion()).join();
        assertEquals(schemaInfo, schemaInfo1);
        // testing with 2 schemas
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        SchemaInfo schemaInfo2 = new SchemaInfo("anygroup1", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        integerGroup.addSchema(schemaInfo2, groupProperties, eTag);
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo2).join();
        SchemaInfo schemaInfo3 = integerGroup.getSchema(versionInfo1.getType(), versionInfo1.getVersion()).join();
        assertEquals(schemaInfo2, schemaInfo3);
        // testing with incorrect input data - getVersionOrdianal will fail
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> integerGroup.getSchema(versionInfo1.getType(), 100).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }
}