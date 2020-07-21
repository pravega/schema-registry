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
import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.common.HashUtil;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;
import io.pravega.schemaregistry.storage.impl.group.records.TableKeySerializer;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import io.pravega.schemaregistry.storage.impl.groups.GroupsValue;
import io.pravega.schemaregistry.storage.impl.groups.PravegaKeyValueGroups;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.schemaregistry.storage.impl.group.PravegaKVGroupTable.TABLE_NAME_FORMAT;
import static io.pravega.schemaregistry.storage.impl.groups.PravegaKeyValueGroups.GROUPS;
import static org.junit.Assert.*;

public class GroupPravegaTest {
    private static final TableRecords.LatestSchemasKey LATEST_SCHEMA_VERSION_KEY =
            new TableRecords.LatestSchemasKey();
    private static final TableRecords.CodecTypesKey CODECS_KEY = new TableRecords.CodecTypesKey();
    private static final TableRecords.ValidationPolicyKey VALIDATION_POLICY_KEY =
            new TableRecords.ValidationPolicyKey();
    private static final TableRecords.GroupPropertyKey GROUP_PROPERTY_KEY = new TableRecords.GroupPropertyKey();
    private ClientConfig clientConfig;
    private String groupName;
    private TableStore tableStore;
    private PravegaKeyValueGroups PravegaKeyValueGroups;
    private ScheduledExecutorService executor;
    String anygroup = "anygroup";
    String anygroup1 = "anygroup1";
    @Before
    public void setUp() {
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        clientConfig = ClientConfig.builder().controllerURI(
                URI.create(pravegaStandaloneUtils.getControllerURI())).build();
        groupName = "mygroup";
        executor = Executors.newScheduledThreadPool(5);
        tableStore = new TableStore(clientConfig, executor);
        PravegaKeyValueGroups = new PravegaKeyValueGroups(tableStore, executor);
    }

    @After
    public void tearDown() {
        PravegaKeyValueGroups.deleteGroup(null, groupName).join();
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        assertEquals(tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord().getState(), GroupsValue.State.Active);
    }

    @Test
    public void testGetCurrentEtag() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        assertEquals(tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord().getState(), GroupsValue.State.Active);
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        Version version = tableStore.getEntry(String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.Etag()),
                x -> TableRecords.fromBytes(TableRecords.Etag.class, x, TableRecords.Etag.class)).join().getVersion();
        assertEquals(etag.etag(), version);
    }

    @Test
    public void testGetLatestSchemas() {
        //null case
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        List<SchemaWithVersion> schemaWithVersionList = PravegaKeyValueGroups.getGroup(
                null, groupName).join().getLatestSchemas().join();
        assertTrue(schemaWithVersionList.isEmpty());
        //non-null case
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        schemaWithVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getLatestSchemas().join();
        assertEquals(1, schemaWithVersionList.size());
        assertEquals(anygroup, schemaWithVersionList.get(0).getSchemaInfo().getType());
        // two schemas
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        schemaWithVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getLatestSchemas().join();
        assertEquals(2, schemaWithVersionList.size());
        assertEquals(anygroup, schemaWithVersionList.get(0).getSchemaInfo().getType());
        assertEquals(anygroup1, schemaWithVersionList.get(1).getSchemaInfo().getType());
    }

    @Test
    public void testAddSchema() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        // one schema
        //versionInfo key
        TableRecords.SchemaRecord schemaRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.SchemaIdKey(0)),
                x -> TableRecords.fromBytes(TableRecords.SchemaIdKey.class, x,
                        TableRecords.SchemaRecord.class)).join().getRecord();
        assertEquals(anygroup, schemaRecord.getSchemaInfo().getType());
        //schemaInfo key
        TableRecords.SchemaVersionList schemaVersionList = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.SchemaFingerprintKey(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()))),
                x -> TableRecords.fromBytes(TableRecords.SchemaFingerprintKey.class, x,
                        TableRecords.SchemaVersionList.class)).join().getRecord();
        assertEquals(1, schemaVersionList.getVersions().size());
        assertEquals(anygroup, schemaVersionList.getVersions().get(0).getType());
        //LatestSchemasKey
        TableRecords.LatestSchemasValue latestSchemaVersionValue = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(LATEST_SCHEMA_VERSION_KEY),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemasKey.class, x,
                        TableRecords.LatestSchemasValue.class)).join().getRecord();
        assertEquals(1, latestSchemaVersionValue.getTypes().size());
        assertTrue(latestSchemaVersionValue.getTypes().containsKey(anygroup));
        assertEquals(0, latestSchemaVersionValue.getTypes().get(anygroup).getLatestId());
        //LatestSchemaVersionForTypeKey
        //multiple schemas
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        VersionInfo versionInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo1).join();

        SchemaInfo schemaInfo2 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo2, groupProperties, etag).join();
        VersionInfo versionInfo2 = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo2).join();
        //versionInfo key
        schemaRecord = tableStore.getEntry(String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.SchemaIdKey(versionInfo1.getId())),
                x -> TableRecords.fromBytes(TableRecords.SchemaIdKey.class, x,
                        TableRecords.SchemaRecord.class)).join().getRecord();
        assertEquals(anygroup, schemaRecord.getSchemaInfo().getType());
        schemaRecord = tableStore.getEntry(String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.SchemaIdKey(versionInfo2.getId())),
                x -> TableRecords.fromBytes(TableRecords.SchemaIdKey.class, x,
                        TableRecords.SchemaRecord.class)).join().getRecord();
        assertEquals(anygroup1, schemaRecord.getSchemaInfo().getType());
        assertEquals(ImmutableMap.of(), schemaRecord.getSchemaInfo().getProperties());
        // schemaInfokey
        schemaVersionList = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.SchemaFingerprintKey(HashUtil.getFingerprint(schemaInfo1.getSchemaData().array()))),
                x -> TableRecords.fromBytes(TableRecords.SchemaFingerprintKey.class, x,
                        TableRecords.SchemaVersionList.class)).join().getRecord();
        assertEquals(3, schemaVersionList.getVersions().size());
        //LatestSchemasKey
        latestSchemaVersionValue = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(LATEST_SCHEMA_VERSION_KEY),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemasKey.class, x,
                        TableRecords.LatestSchemasValue.class)).join().getRecord();
        assertTrue(latestSchemaVersionValue.getTypes().containsKey(anygroup1));
        assertEquals(versionInfo2.getId(), latestSchemaVersionValue.getTypes().get(anygroup1).getLatestId());
    }

    @Test
    public void testGetSchemas() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        byte[] schemaData = new byte[0];

        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());

        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        List<SchemaWithVersion> schemaWitheVersionList = PravegaKeyValueGroups.getGroup(null,
                groupName).join().getSchemas().join();
        assertEquals(1, schemaWitheVersionList.size());
        assertEquals(anygroup, schemaWitheVersionList.get(0).getSchemaInfo().getType());
        //two schemas
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        schemaWitheVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchemas().join();
        assertEquals(2, schemaWitheVersionList.size());
        assertEquals(anygroup, schemaWitheVersionList.get(0).getSchemaInfo().getType());
        assertEquals(anygroup1, schemaWitheVersionList.get(1).getSchemaInfo().getType());
        schemaWitheVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchemas(1).join();
        assertEquals(1, schemaWitheVersionList.size());
        assertEquals(anygroup1, schemaWitheVersionList.get(0).getSchemaInfo().getType());
        schemaWitheVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchemas(0).join();
        assertEquals(2, schemaWitheVersionList.size());
        schemaData = new byte[3];
        SchemaInfo schemaInfo2 = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo2, groupProperties, etag).join();
        schemaWitheVersionList = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchemas(anygroup, 1).join();
        assertEquals(1, schemaWitheVersionList.size());
        assertEquals(ImmutableMap.of(),
                schemaWitheVersionList.get(0).getSchemaInfo().getProperties());
    }

    @Test
    public void testGetVersion() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        SchemaInfo schemaInfo1 = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        VersionInfo versionInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        VersionInfo versionInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo1).join();
        TableRecords.SchemaVersionList schemaVersionList = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.SchemaFingerprintKey(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()))),
                x -> TableRecords.fromBytes(TableRecords.SchemaFingerprintKey.class, x,
                        TableRecords.SchemaVersionList.class)).join().getRecord();
        assertEquals(2, schemaVersionList.getVersions().size());
        assertEquals(versionInfo, schemaVersionList.getVersions().get(0));
        assertEquals(versionInfo1, schemaVersionList.getVersions().get(1));
    }

    @Test
    public void testAddCodecType() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("gzip")).join();
        TableRecords.CodecTypesListValue codecTypesListValue = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(CODECS_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.CodecTypesKey.class, x,
                        TableRecords.CodecTypesListValue.class)).join().getRecord();
        assertEquals(1, codecTypesListValue.getCodecTypes().size());
        assertEquals("gzip", codecTypesListValue.getCodecTypes().get(0));
    }

    @Test
    public void testGetCodecTypes() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        //null case
        List<CodecType> codecTypeList = PravegaKeyValueGroups.getGroup(null, groupName).join().getCodecTypes().join();
        assertEquals(0, codecTypeList.size());
        assertTrue(codecTypeList.isEmpty());
        //non-null
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("gzip")).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("snappy")).join();
        codecTypeList = PravegaKeyValueGroups.getGroup(null, groupName).join().getCodecTypes().join();
        List<String> codecTypes = tableStore.getEntry(String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(CODECS_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.CodecTypesKey.class, x,
                        TableRecords.CodecTypesListValue.class)).join().getRecord().getCodecTypes();
        assertEquals(codecTypeList.size(),codecTypes.size());
        assertEquals(codecTypeList.get(0).getName(), codecTypes.get(0));
        assertEquals(codecTypeList.get(1).getName(), codecTypes.get(1));
    }

    @Test
    public void testCreateEncodingId() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("gzip")).join();
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        EncodingId encodingId = PravegaKeyValueGroups.getGroup(null, groupName).join().createEncodingId(versionInfo, "gzip",
                eTag).join();
        TableRecords.EncodingInfoRecord encodingInfoRecord = new TableRecords.EncodingInfoRecord(versionInfo,
                "gzip");
        TableRecords.EncodingIdRecord encodingIdRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(encodingInfoRecord),
                x -> TableRecords.fromBytes(
                        TableRecords.EncodingInfoRecord.class, x,
                        TableRecords.EncodingIdRecord.class)).join().getRecord();
        assertEquals(encodingId, encodingIdRecord.getEncodingId());
    }

    @Test
    public void testGetEncodingInfo() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("gzip")).join();
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        EncodingId encodingId = PravegaKeyValueGroups.getGroup(null, groupName).join().createEncodingId(versionInfo, "gzip",
                eTag).join();
        EncodingInfo encodingInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getEncodingInfo(encodingId).join();
        assertEquals(new CodecType("gzip"), encodingInfo.getCodecType());
        assertEquals(schemaInfo, encodingInfo.getSchemaInfo());
        assertEquals(versionInfo, encodingInfo.getVersionInfo());
    }

    @Test
    public void testGetEncodingId() {
        //null
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addCodecType(new CodecType("gzip")).join();
        Either<EncodingId, Etag> idEtagEither = PravegaKeyValueGroups.getGroup(null, groupName).join().getEncodingId(
                versionInfo,
                "gzip").join();
        assertTrue(idEtagEither.isRight());
        //non-null
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        EncodingId encodingId = PravegaKeyValueGroups.getGroup(null, groupName).join().createEncodingId(versionInfo, "gzip",
                eTag).join();
        idEtagEither = PravegaKeyValueGroups.getGroup(null, groupName).join().getEncodingId(versionInfo, "gzip").join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId, idEtagEither.getLeft());
        TableRecords.EncodingInfoRecord encodingInfoRecord = new TableRecords.EncodingInfoRecord(versionInfo,
                "gzip");
        TableRecords.EncodingIdRecord encodingIdRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(encodingInfoRecord),
                x -> TableRecords.fromBytes(
                        TableRecords.EncodingInfoRecord.class, x,
                        TableRecords.EncodingIdRecord.class)).join().getRecord();
        assertEquals(encodingId, encodingIdRecord.getEncodingId());
    }

    @Test
    public void testGetLatestSchemaVersion() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        //null case
        assertNull(PravegaKeyValueGroups.getGroup(null, groupName).join().getLatestSchemaVersion().join());
        //non-null
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        SchemaWithVersion schemaWithVersion = PravegaKeyValueGroups.getGroup(
                null, groupName).join().getLatestSchemaVersion().join();
        assertEquals(anygroup1, schemaWithVersion.getSchemaInfo().getType());
        TableRecords.LatestSchemasValue latestSchemaVersionValue = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(LATEST_SCHEMA_VERSION_KEY),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemasKey.class, x,
                        TableRecords.LatestSchemasValue.class)).join().getRecord();
        assertTrue(latestSchemaVersionValue.getTypes().containsKey(schemaWithVersion.getVersionInfo().getType()));
        assertEquals(latestSchemaVersionValue.getTypes().get(anygroup).getLatestVersion(), 1);
        assertEquals(latestSchemaVersionValue.getTypes().get(anygroup).getLatestId(), 1);
        assertEquals(latestSchemaVersionValue.getTypes().get(anygroup1).getLatestVersion(), 0);
        assertEquals(latestSchemaVersionValue.getTypes().get(anygroup1).getLatestId(), 2);
        // withObjectName
        schemaWithVersion = PravegaKeyValueGroups.getGroup(null, groupName).join().getLatestSchemaVersion(anygroup).join();
        assertEquals(ImmutableMap.of(), schemaWithVersion.getSchemaInfo().getProperties());
    }

    @Test
    public void testGetHistory() {
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(), Boolean.TRUE,
                ImmutableMap.of());
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Avro,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        List<GroupHistoryRecord> groupHistoryRecords = PravegaKeyValueGroups.getGroup(null,
                groupName).join().getHistory().join();
        assertEquals(2, groupHistoryRecords.size());
        assertEquals(anygroup, groupHistoryRecords.get(0).getSchemaInfo().getType());
        assertEquals(anygroup1, groupHistoryRecords.get(1).getSchemaInfo().getType());
        // objectType
        byte[] schemaData1 = new byte[10];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Avro, ByteBuffer.wrap(schemaData1),
                ImmutableMap.of());
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        groupHistoryRecords = PravegaKeyValueGroups.getGroup(null, groupName).join().getHistory(anygroup1).join();
        assertEquals(2, groupHistoryRecords.size());
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData).array(),
                groupHistoryRecords.get(0).getSchemaInfo().getSchemaData().array()));
        assertTrue(Arrays.equals(ByteBuffer.wrap(schemaData1).array(),
                groupHistoryRecords.get(1).getSchemaInfo().getSchemaData().array()));
    }

    @Test
    public void testUpdateValidationPolicy() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().updateValidationPolicy(
                Compatibility.forward(), etag).join();
        TableRecords.ValidationRecord validationRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(VALIDATION_POLICY_KEY), x -> TableRecords.fromBytes(
                        TableRecords.ValidationPolicyKey.class, x,
                        TableRecords.ValidationRecord.class)).join().getRecord();
        assertEquals(Compatibility.forward(), validationRecord.getCompatibility());
        // no change
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        Assert.assertNull(PravegaKeyValueGroups.getGroup(null, groupName).join().updateValidationPolicy(
                Compatibility.forward(), etag).join());
    }

    @Test
    public void testGetGroupProperties() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        GroupProperties groupProperties1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getGroupProperties().join();
        TableRecords.ValidationRecord validationRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(VALIDATION_POLICY_KEY), x -> TableRecords.fromBytes(
                        TableRecords.ValidationPolicyKey.class, x,
                        TableRecords.ValidationRecord.class)).join().getRecord();
        TableRecords.GroupPropertiesRecord groupPropertiesRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(GROUP_PROPERTY_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.GroupPropertyKey.class, x,
                        TableRecords.GroupPropertiesRecord.class)).join().getRecord();
        assertEquals(groupProperties1.isAllowMultipleTypes(), groupPropertiesRecord.isAllowMultipleTypes());
        assertEquals(groupProperties1.getCompatibility(), validationRecord.getCompatibility());
        assertEquals(groupProperties1.getSerializationFormat(), groupPropertiesRecord.getSerializationFormat());
    }

    @Test
    public void testDeleteSchema() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo(anygroup, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo(anygroup1, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        Etag etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().deleteSchema(versionInfo.getId(), etag).join();
        TableRecords.VersionDeletedRecord versionDeletedRecordKey = new TableRecords.VersionDeletedRecord(
                versionInfo.getId());
        TableRecords.VersionDeletedRecord versionDeletedRecord = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(versionDeletedRecordKey), x -> TableRecords.fromBytes(
                        TableRecords.VersionDeletedRecord.class, x,
                        TableRecords.VersionDeletedRecord.class)).join().getRecord();
        assertEquals(versionInfo.getId(), versionDeletedRecord.getId());
        // already deleted
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        assertNull(
                PravegaKeyValueGroups.getGroup(null, groupName).join().deleteSchema(versionInfo.getId(), eTag).join());
        // using schemaType and version
        etag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        assertNull(PravegaKeyValueGroups.getGroup(null, groupName).join().deleteSchema(schemaInfo.getType(),
                versionInfo.getVersion(), etag).join());
        // delete schema that does not exist
        eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        Etag finalETag = eTag;
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> PravegaKeyValueGroups.getGroup(null, groupName).join().deleteSchema(200,
                        finalETag).join(), e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchema() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        SchemaInfo schemaInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchema(
                versionInfo1.getId()).join();
        assertEquals(schemaInfo, schemaInfo1);
        TableRecords.TableValue tableValue = tableStore.getEntry(
                String.format(TABLE_NAME_FORMAT, gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.SchemaIdKey(versionInfo1.getId())),
                x -> TableRecords.fromBytes(
                        TableRecords.SchemaIdKey.class, x, TableRecords.SchemaRecord.class)).join().getRecord();
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValue;
        assertEquals(schemaRecord.getSchemaInfo(), schemaInfo);
        // testing for incorrect ordinal value
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> PravegaKeyValueGroups.getGroup(null, groupName).join().getSchema(100).join(),
                e -> e instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void testGetSchemaUsingTypeAndVersion() {
        GroupProperties groupProperties = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.custom("custom1")).compatibility(
                Compatibility.forward()).build();
        PravegaKeyValueGroups.addNewGroup(null, groupName, groupProperties).join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().create(SerializationFormat.Custom,
                ImmutableMap.of(),
                Boolean.TRUE, Compatibility.backward()).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, new NamespaceAndGroup(null, groupName).toBytes(),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SerializationFormat.Custom, ByteBuffer.wrap(schemaData),
                ImmutableMap.of());
        Etag eTag = PravegaKeyValueGroups.getGroup(null, groupName).join().getCurrentEtag().join();
        PravegaKeyValueGroups.getGroup(null, groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getVersion(schemaInfo).join();
        SchemaInfo schemaInfo1 = PravegaKeyValueGroups.getGroup(null, groupName).join().getSchema(versionInfo1.getType(),
                versionInfo1.getVersion()).join();
        assertEquals(schemaInfo, schemaInfo1);
        // test incorrect value of version
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> PravegaKeyValueGroups.getGroup(null, groupName).join().getSchema(versionInfo1.getType(),
                        100).join(), e -> e instanceof RuntimeException);
    }
}