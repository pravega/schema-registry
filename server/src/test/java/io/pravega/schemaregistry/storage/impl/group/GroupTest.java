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
import io.pravega.schemaregistry.common.HashUtil;
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
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
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
    private String groupId;
    private InMemoryGroupTable inMemoryGroupTable;
    private Group<Integer> inMemoryGroup;
    private ScheduledExecutorService executor;
    private String anygroup = "anygroup";
    private String anygroup1 = "anygroup1";

    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(5);
        groupId = "mygroup";
        inMemoryGroupTable = new InMemoryGroupTable();
        inMemoryGroup = new Group<>(inMemoryGroupTable, executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        List<TableRecords.TableValue> recordList = inMemoryGroupTable.getTable().entrySet().stream().map(
                x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(Boolean.TRUE, recordList.contains(
                new TableRecords.ValidationRecord(Compatibility.backward())));
    }

    @Test
    public void testGetCurrentEtag() {
        // null case
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        assertEquals(null, inMemoryGroupTable.fromEtag(eTag));
        assertTrue(inMemoryGroupTable.getTable().isEmpty());
        // non null case
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        List<Integer> integerList = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getValue().getValue().equals(
                        new TableRecords.ValidationRecord(Compatibility.backward()))).map(
                x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(1, integerList.size());
        eTag = inMemoryGroup.getCurrentEtag().join();
        assertEquals(integerList.get(0), inMemoryGroupTable.fromEtag(eTag));
    }

    @Test
    public void testGetTypes() {
        // null case
        List<SchemaWithVersion> schemaWithVersionList = inMemoryGroup.getLatestSchemas().join();
        assertTrue(inMemoryGroupTable.getTable().isEmpty());
        assertEquals(Collections.emptyList(), schemaWithVersionList);
        // non-null case
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.addSchema(schemaInfo1, HashUtil.getFingerprint(schemaInfo1.getSchemaData().array()), groupProperties, eTag).join();
        schemaWithVersionList = inMemoryGroup.getLatestSchemas().join();
        assertEquals(2, schemaWithVersionList.size());
        assertEquals(anygroup, schemaWithVersionList.get(0).getSchemaInfo().getType());
        assertEquals(anygroup1, schemaWithVersionList.get(1).getSchemaInfo().getType());
    }

    @Test
    public void testAddSchema() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        List<TableRecords.TableValue> tableValueListEtag = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.Etag).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListEtag.isEmpty());
        List<TableRecords.TableValue> tableValueListVersionInfo =
                inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertTrue(tableValueListVersionInfo.get(0) instanceof TableRecords.SchemaRecord);
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(0);
        assertEquals(anygroup, schemaRecord.getSchemaInfo().getType());
        assertEquals(0, schemaRecord.getId());

        List<TableRecords.TableValue> tableValueListSchemaInfo =
                inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaFingerprintKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListSchemaInfo.isEmpty());
        assertTrue(tableValueListSchemaInfo.get(0) instanceof TableRecords.SchemaVersionList);
        TableRecords.SchemaVersionList schemaVersionValue =
                (TableRecords.SchemaVersionList) tableValueListSchemaInfo.get(
                        0);
        assertEquals(0, schemaVersionValue.getVersions().get(0).getVersion());

        List<TableRecords.TableValue> tableValueListLatestSchema =
                inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.LatestSchemasKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListLatestSchema.isEmpty());
        assertTrue(tableValueListLatestSchema.get(0) instanceof TableRecords.LatestSchemasValue);
        TableRecords.LatestSchemasValue latestSchemaVersionValue =
                (TableRecords.LatestSchemasValue) tableValueListLatestSchema.get(
                        0);
        assertEquals(0, latestSchemaVersionValue.getTypes().get(anygroup).getLatestVersion());
        assertTrue(latestSchemaVersionValue.getTypes().get(anygroup).getDeletedVersions().isEmpty());
    }

    @Test
    public void testGetSchemas() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.addSchema(schemaInfo1, HashUtil.getFingerprint(schemaInfo1.getSchemaData().array()), groupProperties, eTag).join();
        List<SchemaWithVersion> schemaWithVersionListWithToken = inMemoryGroup.getSchemas().join();
        assertEquals(2, schemaWithVersionListWithToken.size());
        assertEquals(SerializationFormat.Custom,
                schemaWithVersionListWithToken.get(0).getSchemaInfo().getSerializationFormat());
        // with ObjectTypeName
        schemaWithVersionListWithToken = inMemoryGroup.getSchemas(anygroup).join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        schemaWithVersionListWithToken = inMemoryGroup.getSchemas(anygroup1).join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        List<TableRecords.TableValue> tableValueListVersionInfo =
                inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(2, tableValueListVersionInfo.size());
        assertFalse(tableValueListVersionInfo.isEmpty());
        List<Integer> version = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaIdKey).map(x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(2, version.size());
        List<TableRecords.TableKey> tableKeyObjectTypes = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.LatestSchemasKey).map(
                x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, tableKeyObjectTypes.size());
        TableRecords.LatestSchemasValue objectTypesListValue = inMemoryGroupTable.getEntry(tableKeyObjectTypes.get(0),
                TableRecords.LatestSchemasValue.class).join();
        assertEquals(2, objectTypesListValue.getTypes().size());
    }

    @Test
    public void testGetVersion() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.addSchema(schemaInfo1, HashUtil.getFingerprint(schemaInfo1.getSchemaData().array()), groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), x -> true).join();
        VersionInfo versionInfo1 = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo1.getSchemaData().array()), x -> true).join();
        List<TableRecords.TableValue> tableValueListVersionInfo =
                inMemoryGroupTable.getTable().entrySet().stream().filter(
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
        inMemoryGroup.addCodecType(new CodecType("gzip")).join();
        List<TableRecords.TableKey> codecsListKey = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecTypesKey).map(x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecTypesListValue codecsListValue = inMemoryGroupTable.getEntry(codecsListKey.get(0),
                TableRecords.CodecTypesListValue.class).join();
        assertEquals("gzip", codecsListValue.getCodecTypes().get(0));
    }

    @Test
    public void testCreateEncodingId() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        inMemoryGroup.addCodecType(new CodecType("gzip")).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        EncodingId encodingId = inMemoryGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        inMemoryGroup.addCodecType(new CodecType("snappy")).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        EncodingId encodingId1 = inMemoryGroup.createEncodingId(versionInfo, "snappy", eTag).join();
        List<TableRecords.TableValue> encodingInfoRecordList = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        List<TableRecords.TableKey> encodingIdObtained = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getKey()).collect(
                Collectors.toList());
        System.out.println(encodingInfoRecordList);
        assertEquals(2, encodingInfoRecordList.size());
        assertEquals(new TableRecords.EncodingIdRecord(encodingId), encodingIdObtained.get(0));
        assertEquals(new TableRecords.EncodingIdRecord(encodingId1), encodingIdObtained.get(1));
    }

    @Test
    public void testGetEncodingInfo() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        inMemoryGroup.addCodecType(new CodecType("gzip")).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        EncodingId encodingId = inMemoryGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        EncodingInfo encodingInfo = inMemoryGroup.getEncodingInfo(encodingId).join();
        assertEquals(new CodecType("gzip"), encodingInfo.getCodecType());
        assertEquals(versionInfo, encodingInfo.getVersionInfo());
        assertEquals(SerializationFormat.Custom, encodingInfo.getSchemaInfo().getSerializationFormat());
    }

    @Test
    public void testGetLatestSchemaVersion() {
        // null case
        SchemaWithVersion schemaWithVersion = inMemoryGroup.getLatestSchemaVersion().join();
        assertNull(schemaWithVersion);
        // non-null case
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        schemaWithVersion = inMemoryGroup.getLatestSchemaVersion().join();
        assertEquals(versionInfo, schemaWithVersion.getVersionInfo());
        assertEquals(anygroup, schemaWithVersion.getSchemaInfo().getType());

        // objectTypeName
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint1 = HashUtil.getFingerprint(schemaInfo1.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo1, fingerprint1, groupProperties, eTag).join();
        VersionInfo versionInfo1 = inMemoryGroup.getVersion(fingerprint1, x -> true).join();
        // null
        schemaWithVersion = inMemoryGroup.getLatestSchemaVersion("anygroup2").join();
        assertNull(schemaWithVersion);
        // non-null
        schemaWithVersion = inMemoryGroup.getLatestSchemaVersion(anygroup1).join();
        assertEquals(versionInfo1, schemaWithVersion.getVersionInfo());
        schemaWithVersion = inMemoryGroup.getLatestSchemaVersion(anygroup).join();
        assertTrue(Arrays.equals(schemaInfo.getSchemaData().array(),
                schemaWithVersion.getSchemaInfo().getSchemaData().array()));
    }

    @Test
    public void testGetCodecTypes() {
        // null
        List<CodecType> codecTypeList = inMemoryGroup.getCodecTypes().join();
        assertEquals(0, codecTypeList.size());
        assertTrue(codecTypeList.isEmpty());
        // non-null
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.addCodecType(new CodecType("snappy")).join();
        inMemoryGroup.addCodecType(new CodecType("gzip")).join();
        codecTypeList = inMemoryGroup.getCodecTypes().join();
        List<TableRecords.TableKey> codecsListKey = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecTypesKey).map(x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecTypesListValue codecsListValue = inMemoryGroupTable.getEntry(codecsListKey.get(0),
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
        inMemoryGroup.create(SerializationFormat.Avro, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(), Boolean.TRUE,
                ImmutableMap.of());
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        List<GroupHistoryRecord> groupHistoryRecords = inMemoryGroup.getHistory().join();
        assertEquals(2, groupHistoryRecords.size());
        assertEquals(anygroup, groupHistoryRecords.get(0).getSchemaInfo().getType());
        assertEquals(anygroup1, groupHistoryRecords.get(1).getSchemaInfo().getType());
        // objectType
        byte[] schemaData1 = new byte[10];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Avro, ByteBuffer.wrap(schemaData1),
                ImmutableMap.of());
        eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        groupHistoryRecords = inMemoryGroup.getHistory(anygroup1).join();
        assertEquals(2, groupHistoryRecords.size());
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData).array(),
                groupHistoryRecords.get(0).getSchemaInfo().getSchemaData().array()));
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData1).array(),
                groupHistoryRecords.get(1).getSchemaInfo().getSchemaData().array()));
    }

    @Test
    public void testUpdateValidationPolicy() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.updateValidationPolicy(Compatibility.forward(), eTag).join();
        List<TableRecords.TableValue> validationRecord = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.ValidationPolicyKey).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(1, validationRecord.size());
        TableRecords.ValidationRecord validationRecord1 = (TableRecords.ValidationRecord) validationRecord.get(0);
        assertEquals(Compatibility.forward(), validationRecord1.getCompatibility());
        // when unchanged
        eTag = inMemoryGroup.getCurrentEtag().join();
        assertNull(inMemoryGroup.updateValidationPolicy(Compatibility.forward(), eTag).join());
    }

    @Test
    public void testGetGroupProperties() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        GroupProperties groupProperties = inMemoryGroup.getGroupProperties().join();
        assertEquals(SerializationFormat.Custom, groupProperties.getSerializationFormat());
        assertEquals(Compatibility.backward(), groupProperties.getCompatibility());
        assertTrue(groupProperties.isAllowMultipleTypes());
        assertEquals(ImmutableMap.of(), groupProperties.getProperties());
    }

    @Test
    public void testGetEncodingId() {
        //null
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), x -> true).join();
        inMemoryGroup.addCodecType(new CodecType("gzip")).join();
        Either<EncodingId, Etag> idEtagEither = inMemoryGroup.getEncodingId(versionInfo, "gzip").join();
        assertTrue(idEtagEither.isRight());
        //non-null
        eTag = inMemoryGroup.getCurrentEtag().join();
        EncodingId encodingId = inMemoryGroup.createEncodingId(versionInfo, "gzip", eTag).join();
        inMemoryGroup.addCodecType(new CodecType("snappy")).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag);
        VersionInfo versionInfo1 = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), x -> true).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        EncodingId encodingId1 = inMemoryGroup.createEncodingId(versionInfo1, "snappy", eTag).join();
        idEtagEither = inMemoryGroup.getEncodingId(versionInfo, "gzip").join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId, idEtagEither.getLeft());
        idEtagEither = inMemoryGroup.getEncodingId(versionInfo1, "snappy").join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId1, idEtagEither.getLeft());
    }

    @Test
    public void testDeleteSchema() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Protobuf).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo, HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), groupProperties, eTag);
        VersionInfo versionInfo = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()), x -> true).join();
        eTag = inMemoryGroup.getCurrentEtag().join();
        inMemoryGroup.deleteSchema(versionInfo.getId(), eTag).join();
        List<TableRecords.TableValue> deletedOrdinalList = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionDeletedRecord).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(1, deletedOrdinalList.size());
        TableRecords.VersionDeletedRecord versionDeletedRecord =
                (TableRecords.VersionDeletedRecord) deletedOrdinalList.get(
                        0);
        assertEquals(versionInfo.getId(), versionDeletedRecord.getId());
        // already deleted
        eTag = inMemoryGroup.getCurrentEtag().join();
        assertNull(inMemoryGroup.deleteSchema(versionInfo.getId(), eTag).join());
    }

    @Test
    public void testGetSchema() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        SchemaInfo schemaInfo1 = inMemoryGroup.getSchema(versionInfo.getId()).join();
        assertEquals(schemaInfo, schemaInfo1);
        List<TableRecords.TableValue> schemaRecordValues = inMemoryGroupTable.getTable().entrySet().stream().filter(
                x -> x.getKey().equals(new TableRecords.SchemaIdKey(versionInfo.getId()))).map(
                x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(1, schemaRecordValues.size());
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) schemaRecordValues.get(0);
        assertEquals(schemaRecord.getSchemaInfo(), schemaInfo);
        // trying with an incorrect value of ordinal
        AssertExtensions.assertThrows("An exception should have been thrown", () -> inMemoryGroup.getSchema(100).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchemaUsingTypeAndVersion() {
        inMemoryGroup.create(SerializationFormat.Custom, ImmutableMap.of(), Boolean.TRUE,
                Compatibility.backward()).join();
        Etag eTag = inMemoryGroup.getCurrentEtag().join();
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Custom).compatibility(
                Compatibility.forward()).build();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        BigInteger fingerprint = HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
        inMemoryGroup.addSchema(schemaInfo, fingerprint, groupProperties, eTag).join();
        VersionInfo versionInfo = inMemoryGroup.getVersion(fingerprint, x -> true).join();
        SchemaInfo schemaInfo1 = inMemoryGroup.getSchema(versionInfo.getType(), versionInfo.getVersion()).join();
        assertEquals(schemaInfo, schemaInfo1);
        // testing with 2 schemas
        eTag = inMemoryGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        SchemaInfo schemaInfo2 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        inMemoryGroup.addSchema(schemaInfo2, HashUtil.getFingerprint(schemaInfo2.getSchemaData().array()), groupProperties, eTag);
        VersionInfo versionInfo1 = inMemoryGroup.getVersion(HashUtil.getFingerprint(schemaInfo2.getSchemaData().array()), x -> true).join();
        SchemaInfo schemaInfo3 = inMemoryGroup.getSchema(versionInfo1.getType(), versionInfo1.getVersion()).join();
        assertEquals(schemaInfo2, schemaInfo3);
        // testing with incorrect input data - getVersionOrdianal will fail
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> inMemoryGroup.getSchema(versionInfo1.getType(), 100).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }
}