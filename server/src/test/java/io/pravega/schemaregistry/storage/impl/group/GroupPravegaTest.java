/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.client.ClientConfig;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.TableKeySerializer;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import io.pravega.schemaregistry.storage.impl.groups.GroupsValue;
import io.pravega.schemaregistry.storage.impl.groups.PravegaKVGroups;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;

public class GroupPravegaTest {
    private static final TableRecords.SchemaNamesKey SCHEMA_NAMES_KEY = new TableRecords.SchemaNamesKey();
    private static final TableKeySerializer KEY_SERIALIZER = new TableKeySerializer();
    private static final HashFunction HASH = Hashing.murmur3_128();
    private static final TableRecords.LatestSchemaVersionKey LATEST_SCHEMA_VERSION_KEY =
            new TableRecords.LatestSchemaVersionKey();
    private ScheduledExecutorService executor;
    private static final TableRecords.CodecsKey CODECS_KEY = new TableRecords.CodecsKey();
    private static final TableRecords.ValidationPolicyKey VALIDATION_POLICY_KEY =
            new TableRecords.ValidationPolicyKey();
    private static final TableRecords.GroupPropertyKey GROUP_PROPERTY_KEY = new TableRecords.GroupPropertyKey();
    String groupName;
    ClientConfig clientConfig;
    PravegaKVGroupTable pravegaKVGroupTable;
    TableStore tableStore;
    GrpcAuthHelper grpcAuthHelper;
    Group<Version> group;
    PravegaKVGroups pravegaKVGroups;
    static final String GROUPS = "schema-registry/groups/0";

    @Before
    public void setUp() {
        grpcAuthHelper = new GrpcAuthHelper(Boolean.FALSE, null, null);
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        clientConfig = ClientConfig.builder().controllerURI(
                URI.create(pravegaStandaloneUtils.getControllerURI())).build();
        groupName = "mygroup";
        executor = Executors.newScheduledThreadPool(5);
        tableStore = new TableStore(clientConfig, () -> "", executor);
        //pravegaKVGroupTable = new PravegaKVGroupTable(groupName, "tableStore", tableStore);
        pravegaKVGroups = new PravegaKVGroups(tableStore, executor);
        //group = new Group<>(groupName, pravegaKVGroupTable,executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        assertTrue(tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord().getState().equals(GroupsValue.State.Active));
    }

    @Test
    public void testGetCurrentEtag() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        assertTrue(tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord().getState().equals(GroupsValue.State.Active));
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        Version version = tableStore.getEntry(String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.Etag()),
                x -> TableRecords.fromBytes(TableRecords.Etag.class, x, TableRecords.Etag.class)).join().getVersion();
        assertEquals(etag.etag(), version);
    }

    @Test
    public void testGetSchemaNames() {
        //null case
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        List<String> schemaNamesList = pravegaKVGroups.getGroup(groupName).join().getSchemaNames().join();
        assertTrue(schemaNamesList.isEmpty());
        //non-null case
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        schemaNamesList = pravegaKVGroups.getGroup(groupName).join().getSchemaNames().join();
        assertEquals(2, schemaNamesList.size());
        assertEquals("anygroup", schemaNamesList.get(0));
        assertEquals("anygroup1", schemaNamesList.get(1));
    }

    @Test
    public void testAddSchema() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        // one schema
        //versionInfo key
        TableRecords.SchemaRecord schemaRecord = tableStore.getEntry(String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(new TableRecords.VersionKey(0)),
                x -> TableRecords.fromBytes(TableRecords.VersionKey.class, x,
                        TableRecords.SchemaRecord.class)).join().getRecord();
        assertEquals("anygroup", schemaRecord.getSchemaInfo().getName());
        //schemaInfo key
        TableRecords.SchemaVersionList schemaVersionList = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.SchemaFingerprintKey(HASH.hashBytes(schemaInfo.getSchemaData()).asLong())),
                x -> TableRecords.fromBytes(TableRecords.SchemaFingerprintKey.class, x,
                        TableRecords.SchemaVersionList.class)).join().getRecord();
        assertEquals(1, schemaVersionList.getVersions().size());
        assertEquals("anygroup", schemaVersionList.getVersions().get(0).getSchemaName());
        //LatestSchemaVersionKey
        TableRecords.LatestSchemaVersionValue latestSchemaVersionValue = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(LATEST_SCHEMA_VERSION_KEY),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemaVersionKey.class, x,
                        TableRecords.LatestSchemaVersionValue.class)).join().getRecord();
        assertEquals("anygroup", latestSchemaVersionValue.getVersion().getSchemaName());
        assertEquals(0, latestSchemaVersionValue.getVersion().getOrdinal());
        //LatestSchemaVersionForSchemaNameKey
        TableRecords.LatestSchemaVersionValue latestSchemaVersionValue1 = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.LatestSchemaVersionForSchemaNameKey(schemaInfo.getName())),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemaVersionForSchemaNameKey.class, x,
                        TableRecords.LatestSchemaVersionValue.class)).join().getRecord();
        assertTrue(latestSchemaVersionValue.equals(latestSchemaVersionValue1));
        // ObjectTypes/SchemaNames key
        TableRecords.SchemaNamesListValue schemaNamesListValue = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(SCHEMA_NAMES_KEY),
                x -> TableRecords.fromBytes(TableRecords.SchemaNamesKey.class, x,
                        TableRecords.SchemaNamesListValue.class)).join().getRecord();
        assertEquals(1, schemaNamesListValue.getSchemaNames().size());
        assertEquals("anygroup", schemaNamesListValue.getSchemaNames().get(0));
    }

    @Test
    public void testGetSchemas() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        List<SchemaWithVersion> schemaWitheVersionList = pravegaKVGroups.getGroup(groupName).join().getSchemas().join();
        assertEquals(2, schemaWitheVersionList.size());
        assertEquals("anygroup", schemaWitheVersionList.get(0).getSchema().getName());
        assertEquals("anygroup1", schemaWitheVersionList.get(1).getSchema().getName());
        schemaWitheVersionList = pravegaKVGroups.getGroup(groupName).join().getSchemas(1).join();
        assertEquals(1, schemaWitheVersionList.size());
        assertEquals("anygroup1", schemaWitheVersionList.get(0).getSchema().getName());
        schemaWitheVersionList = pravegaKVGroups.getGroup(groupName).join().getSchemas(0).join();
        assertEquals(2, schemaWitheVersionList.size());
        schemaData = new byte[3];
        SchemaInfo schemaInfo2 = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key1", "value1"));
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo2, groupProperties, etag).join();
        schemaWitheVersionList = pravegaKVGroups.getGroup(groupName).join().getSchemas("anygroup", 1).join();
        assertEquals(1, schemaWitheVersionList.size());
        assertEquals(Collections.singletonMap("key1", "value1"),
                schemaWitheVersionList.get(0).getSchema().getProperties());
    }

    @Test
    public void testGetVersion() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, etag).join();
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo1, groupProperties, etag).join();
        VersionInfo versionInfo = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo).join();
        VersionInfo versionInfo1 = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo1).join();
        TableRecords.SchemaVersionList schemaVersionList = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(
                        new TableRecords.SchemaFingerprintKey(HASH.hashBytes(schemaInfo.getSchemaData()).asLong())),
                x -> TableRecords.fromBytes(TableRecords.SchemaFingerprintKey.class, x,
                        TableRecords.SchemaVersionList.class)).join().getRecord();
        assertEquals(2, schemaVersionList.getVersions().size());
        assertTrue(versionInfo.equals(schemaVersionList.getVersions().get(0)));
        assertTrue(versionInfo1.equals(schemaVersionList.getVersions().get(1)));
    }

    @Test
    public void testAddCodec() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.GZip).join();
        TableRecords.CodecsListValue codecsListValue = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(CODECS_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.CodecsKey.class, x, TableRecords.CodecsListValue.class)).join().getRecord();
        assertEquals(1, codecsListValue.getCodecs().size());
        assertEquals(CodecType.GZip, codecsListValue.getCodecs().get(0));
    }

    @Test
    public void testGetCodecTypes() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        //null case
        List<CodecType> codecTypeList = pravegaKVGroups.getGroup(groupName).join().getCodecTypes().join();
        assertEquals(1, codecTypeList.size());
        assertEquals(CodecType.None, codecTypeList.get(0));
        //non-null
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.GZip).join();
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.Snappy).join();
        codecTypeList = pravegaKVGroups.getGroup(groupName).join().getCodecTypes().join();
        List<CodecType> codecTypes = tableStore.getEntry(String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(CODECS_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.CodecsKey.class, x,
                        TableRecords.CodecsListValue.class)).join().getRecord().getCodecs();
        assertTrue(codecTypeList.equals(codecTypes));
    }

    @Test
    public void testCreateEncodingId() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo).join();
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.GZip).join();
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        EncodingId encodingId = pravegaKVGroups.getGroup(groupName).join().createEncodingId(versionInfo, CodecType.GZip,
                eTag).join();
        TableRecords.EncodingInfoRecord encodingInfoRecord = new TableRecords.EncodingInfoRecord(versionInfo,
                CodecType.GZip);
        TableRecords.EncodingIdRecord encodingIdRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(encodingInfoRecord),
                x -> TableRecords.fromBytes(
                        TableRecords.EncodingInfoRecord.class, x,
                        TableRecords.EncodingIdRecord.class)).join().getRecord();
        assertEquals(encodingId, encodingIdRecord.getEncodingId());
    }

    @Test
    public void testGetEncodingInfo() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo).join();
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.GZip).join();
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        EncodingId encodingId = pravegaKVGroups.getGroup(groupName).join().createEncodingId(versionInfo, CodecType.GZip,
                eTag).join();
        EncodingInfo encodingInfo = pravegaKVGroups.getGroup(groupName).join().getEncodingInfo(encodingId).join();
        assertEquals(CodecType.GZip, encodingInfo.getCodec());
        assertEquals(schemaInfo, encodingInfo.getSchemaInfo());
        assertEquals(versionInfo, encodingInfo.getVersionInfo());
    }

    @Test
    public void testGetEncodingId() {
        //null
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo).join();
        pravegaKVGroups.getGroup(groupName).join().addCodec(CodecType.GZip).join();
        Either<EncodingId, Etag> idEtagEither = pravegaKVGroups.getGroup(groupName).join().getEncodingId(versionInfo,
                CodecType.GZip).join();
        assertTrue(idEtagEither.isRight());
        //non-null
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        EncodingId encodingId = pravegaKVGroups.getGroup(groupName).join().createEncodingId(versionInfo, CodecType.GZip,
                eTag).join();
        idEtagEither = pravegaKVGroups.getGroup(groupName).join().getEncodingId(versionInfo, CodecType.GZip).join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId, idEtagEither.getLeft());
        TableRecords.EncodingInfoRecord encodingInfoRecord = new TableRecords.EncodingInfoRecord(versionInfo,
                CodecType.GZip);
        TableRecords.EncodingIdRecord encodingIdRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(encodingInfoRecord),
                x -> TableRecords.fromBytes(
                        TableRecords.EncodingInfoRecord.class, x,
                        TableRecords.EncodingIdRecord.class)).join().getRecord();
        assertTrue(encodingId.equals(encodingIdRecord.getEncodingId()));
    }

    @Test
    public void testGetLatestSchemaVersion() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        //null case
        assertEquals(null, pravegaKVGroups.getGroup(groupName).join().getLatestSchemaVersion().join());
        //non-null
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        schemaInfo = new SchemaInfo("anygroup", SchemaType.Custom, schemaData,
                Collections.singletonMap("key1", "value1"));
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Custom, schemaData,
                Collections.singletonMap("key", "value"));
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        SchemaWithVersion schemaWithVersion = pravegaKVGroups.getGroup(
                groupName).join().getLatestSchemaVersion().join();
        assertEquals("anygroup1", schemaWithVersion.getSchema().getName());
        TableRecords.LatestSchemaVersionValue latestSchemaVersionValue = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(LATEST_SCHEMA_VERSION_KEY),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemaVersionKey.class, x,
                        TableRecords.LatestSchemaVersionValue.class)).join().getRecord();
        assertTrue(latestSchemaVersionValue.getVersion().equals(schemaWithVersion.getVersion()));
        // withObjectName
        schemaWithVersion = pravegaKVGroups.getGroup(groupName).join().getLatestSchemaVersion("anygroup").join();
        assertEquals(Collections.singletonMap("key1", "value1"), schemaWithVersion.getSchema().getProperties());
        TableRecords.LatestSchemaVersionForSchemaNameKey key = new TableRecords.LatestSchemaVersionForSchemaNameKey(
                "anygroup");
        latestSchemaVersionValue = tableStore.getEntry(String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(key),
                x -> TableRecords.fromBytes(TableRecords.LatestSchemaVersionForSchemaNameKey.class, x,
                        TableRecords.LatestSchemaVersionValue.class)).join().getRecord();
        assertTrue(latestSchemaVersionValue.getVersion().equals(schemaWithVersion.getVersion()));
    }

    @Test
    public void testGetHistory() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Avro, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Avro, schemaData,
                Collections.singletonMap("key", "value"));
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Avro, schemaData,
                Collections.singletonMap("key1", "value1"));
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        List<GroupHistoryRecord> groupHistoryRecords = pravegaKVGroups.getGroup(groupName).join().getHistory().join();
        assertEquals(2, groupHistoryRecords.size());
        assertEquals("anygroup", groupHistoryRecords.get(0).getSchema().getName());
        assertEquals("anygroup1", groupHistoryRecords.get(1).getSchema().getName());
        // objectType
        byte[] schemaData1 = new byte[10];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Avro, schemaData1,
                Collections.singletonMap("key1", "value1"));
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        groupHistoryRecords = pravegaKVGroups.getGroup(groupName).join().getHistory("anygroup1").join();
        assertEquals(2, groupHistoryRecords.size());
        assertTrue(Arrays.equals(schemaData, groupHistoryRecords.get(0).getSchema().getSchemaData()));
        assertTrue(Arrays.equals(schemaData1, groupHistoryRecords.get(1).getSchema().getSchemaData()));
    }

    @Test
    public void testUpdateValidationPolicy() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().updateValidationPolicy(
                SchemaValidationRules.of(Compatibility.forward()), etag).join();
        TableRecords.ValidationRecord validationRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(VALIDATION_POLICY_KEY), x -> TableRecords.fromBytes(
                        TableRecords.ValidationPolicyKey.class, x,
                        TableRecords.ValidationRecord.class)).join().getRecord();
        assertEquals(SchemaValidationRules.of(Compatibility.forward()), validationRecord.getValidationRules());
        // no change
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        Assert.assertNull(pravegaKVGroups.getGroup(groupName).join().updateValidationPolicy(
                SchemaValidationRules.of(Compatibility.forward()), etag).join());
    }

    @Test
    public void testGetGroupProperties() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        GroupProperties groupProperties1 = pravegaKVGroups.getGroup(groupName).join().getGroupProperties().join();
        TableRecords.ValidationRecord validationRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(VALIDATION_POLICY_KEY), x -> TableRecords.fromBytes(
                        TableRecords.ValidationPolicyKey.class, x,
                        TableRecords.ValidationRecord.class)).join().getRecord();
        TableRecords.GroupPropertiesRecord groupPropertiesRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()), new TableKeySerializer().toBytes(GROUP_PROPERTY_KEY),
                x -> TableRecords.fromBytes(
                        TableRecords.GroupPropertyKey.class, x,
                        TableRecords.GroupPropertiesRecord.class)).join().getRecord();
        GroupProperties groupProperties2 = new GroupProperties(groupPropertiesRecord.getSchemaType(),
                validationRecord.getValidationRules(), groupPropertiesRecord.isVersionedBySchemaName(),
                groupPropertiesRecord.getProperties());
        assertEquals(groupProperties2, groupProperties1);
    }

    @Test
    public void testDeleteSchema() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        byte[] schemaData = new byte[3];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Avro, schemaData,
                Collections.singletonMap("key", "value"));
        Etag eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        eTag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Avro, schemaData,
                Collections.singletonMap("key1", "value1"));
        pravegaKVGroups.getGroup(groupName).join().addSchema(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = pravegaKVGroups.getGroup(groupName).join().getVersion(schemaInfo).join();
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        pravegaKVGroups.getGroup(groupName).join().deleteSchema(versionInfo.getOrdinal(), etag).join();
        TableRecords.VersionDeletedRecord versionDeletedRecordKey = new TableRecords.VersionDeletedRecord(
                versionInfo.getOrdinal());
        TableRecords.VersionDeletedRecord versionDeletedRecord = tableStore.getEntry(
                String.format("table-%s/metadata/0", gv.getId()),
                new TableKeySerializer().toBytes(versionDeletedRecordKey), x -> TableRecords.fromBytes(
                        TableRecords.VersionDeletedRecord.class, x,
                        TableRecords.VersionDeletedRecord.class)).join().getRecord();
        assertEquals(versionInfo.getOrdinal(), versionDeletedRecord.getOrdinal());
        // already deleted
        etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        assertNull(pravegaKVGroups.getGroup(groupName).join().deleteSchema(versionInfo.getOrdinal(), eTag).join());
    }
}
















