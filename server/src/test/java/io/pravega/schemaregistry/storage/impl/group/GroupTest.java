package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class GroupTest {
    private ScheduledExecutorService executor;
    Group<Integer> integerGroup;
    String groupId;
    InMemoryGroupTable integerGroupTable;

    @Before
    public void setup() {
        executor = Executors.newScheduledThreadPool(5);
        groupId = "mygroup";
        integerGroupTable = new InMemoryGroupTable();
        integerGroup = new Group<>(groupId, integerGroupTable, executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        List<TableRecords.TableValue> recordList = integerGroupTable.table.entrySet().stream().map(
                x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(Boolean.TRUE, recordList.contains(
                new TableRecords.ValidationRecord(SchemaValidationRules.of(Compatibility.backward()))));
    }

    @Test
    public void testGetCurrentEtag() {
        // null case
        Etag eTag = integerGroup.getCurrentEtag().join();
        assertEquals(null, integerGroupTable.fromEtag(eTag));
        assertTrue(integerGroupTable.table.isEmpty());
        // non null case
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        List<Integer> integerList = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getValue().getValue().equals(
                        new TableRecords.ValidationRecord(SchemaValidationRules.of(Compatibility.backward())))).map(
                x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(1, integerList.size());
        eTag = integerGroup.getCurrentEtag().join();
        assertEquals(integerList.get(0), integerGroupTable.fromEtag(eTag));
    }

    @Test
    public void testGetObjectTypes() {
        // null case
        List<String> stringListWithToken = integerGroup.getObjectTypes().join();
        assertTrue(integerGroupTable.table.isEmpty());
        assertEquals(Collections.emptyList(), stringListWithToken);
        // non-null case
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchemaToGroup(schemaInfo1, groupProperties, eTag).join();
        stringListWithToken = integerGroup.getObjectTypes().join();
        assertEquals(2, stringListWithToken.size());
        assertEquals("anygroup", stringListWithToken.get(0));
        assertEquals("anygroup1", stringListWithToken.get(1));

    }

    @Test
    public void testAddSchemaToGroup() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        List<TableRecords.TableValue> tableValueListEtag = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.Etag).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListEtag.isEmpty());

        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertTrue(tableValueListVersionInfo.get(0) instanceof TableRecords.SchemaRecord);
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(0);
        assertEquals("mygroup", schemaRecord.getSchemaInfo().getName());
        assertEquals(0, schemaRecord.getVersionInfo().getOrdinal());

        List<TableRecords.TableValue> tableValueListSchemaInfo = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.SchemaInfoKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListSchemaInfo.isEmpty());
        assertTrue(tableValueListSchemaInfo.get(0) instanceof TableRecords.SchemaVersionValue);
        TableRecords.SchemaVersionValue schemaVersionValue =
                (TableRecords.SchemaVersionValue) tableValueListSchemaInfo.get(
                0);
        assertEquals(0, schemaVersionValue.getVersions().get(0).getVersion());

        List<TableRecords.TableValue> tableValueListLatestSchema = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.LatestSchemaVersionForObjectTypeKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListLatestSchema.isEmpty());
        assertTrue(tableValueListLatestSchema.get(0) instanceof TableRecords.LatestSchemaVersionValue);
        TableRecords.LatestSchemaVersionValue latestSchemaVersionValue =
                (TableRecords.LatestSchemaVersionValue) tableValueListLatestSchema.get(
                0);
        assertEquals(0, latestSchemaVersionValue.getVersion().getVersion());

        List<TableRecords.TableValue> tableValueListObjectTypesList =
                integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.ObjectTypesKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListObjectTypesList.isEmpty());
        assertTrue(tableValueListObjectTypesList.get(0) instanceof TableRecords.ObjectTypesListValue);
        TableRecords.ObjectTypesListValue objectTypesListValue =
                (TableRecords.ObjectTypesListValue) tableValueListObjectTypesList.get(
                0);
        assertEquals(1, tableValueListObjectTypesList.size());
        assertEquals("mygroup", objectTypesListValue.getObjectTypes().get(0));
    }

    @Test
    public void testGetSchemas() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchemaToGroup(schemaInfo1, groupProperties, eTag).join();
        List<SchemaWithVersion> schemaWithVersionListWithToken = integerGroup.getSchemas().join();
        assertEquals(2, schemaWithVersionListWithToken.size());
        assertEquals(SchemaType.Protobuf, schemaWithVersionListWithToken.get(0).getSchema().getSchemaType());
        // with ObjectTypeName
        schemaWithVersionListWithToken = integerGroup.getSchemas("anygroup").join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        schemaWithVersionListWithToken = integerGroup.getSchemas("anygroup1").join();
        assertEquals(1, schemaWithVersionListWithToken.size());
        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertEquals(2, tableValueListVersionInfo.size());
        assertFalse(tableValueListVersionInfo.isEmpty());
        List<Integer> version = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionKey).map(x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertEquals(2, version.size());
        List<TableRecords.TableKey> tableKeyObjectTypes = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.ObjectTypesKey).map(
                x -> x.getKey()).collect(
                Collectors.toList());
        assertEquals(1, tableKeyObjectTypes.size());
        TableRecords.ObjectTypesListValue objectTypesListValue = integerGroupTable.getEntry(tableKeyObjectTypes.get(0),
                TableRecords.ObjectTypesListValue.class).join();
        assertEquals(2, objectTypesListValue.getObjectTypes().size());
    }

    @Test
    public void testGetVersion() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchemaToGroup(schemaInfo1, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo1).join();
        List<Integer> tableValueListVersionInfo = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.VersionKey).map(
                x -> x.getValue().getVersion()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertEquals(2, tableValueListVersionInfo.size());
        assertEquals(tableValueListVersionInfo.get(0).intValue(), versionInfo.getVersion());
        assertEquals(tableValueListVersionInfo.get(1).intValue(), versionInfo1.getVersion());
    }

    @Test
    public void testAddCodec() {
        integerGroup.addCodec(CodecType.GZip).join();
        List<TableRecords.TableKey> codecsListKey = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecsKey).map(x -> x.getKey()).collect(Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecsListValue codecsListValue = integerGroupTable.getEntry(codecsListKey.get(0),
                TableRecords.CodecsListValue.class).join();
        assertEquals(CodecType.GZip, codecsListValue.getCodecs().get(0));
    }

    @Test
    public void testCreateEncodingId() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodec(CodecType.GZip).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, CodecType.GZip, eTag).join();
        integerGroup.addCodec(CodecType.Snappy).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key1", "value1"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        versionInfo = integerGroup.getVersion(schemaInfo).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId1 = integerGroup.createEncodingId(versionInfo, CodecType.Snappy, eTag).join();
        List<TableRecords.TableValue> encodingInfoRecordList = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getValue().getValue()).collect(
                Collectors.toList());
        List<TableRecords.TableKey> encodingIdObtained = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.EncodingIdRecord).map(x -> x.getKey()).collect(
                Collectors.toList());
        System.out.println(encodingInfoRecordList);
        assertEquals(2, encodingInfoRecordList.size());
        assertEquals(new TableRecords.EncodingIdRecord(encodingId), encodingIdObtained.get(0));
        assertEquals(new TableRecords.EncodingIdRecord(encodingId1), encodingIdObtained.get(1));
    }

    @Test
    public void testGetEncodingInfo() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodec(CodecType.GZip).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, CodecType.GZip, eTag).join();
        EncodingInfo encodingInfo = integerGroup.getEncodingInfo(encodingId).join();
        assertEquals(CodecType.GZip, encodingInfo.getCodec());
        assertEquals(versionInfo, encodingInfo.getVersionInfo());
        assertEquals(SchemaType.Protobuf, encodingInfo.getSchemaInfo().getSchemaType());
    }

    @Test
    public void testGetLatestSchema() {
        // null case
        SchemaWithVersion schemaWithVersion = integerGroup.getLatestSchema().join();
        assertEquals(null, schemaWithVersion);
        // non-null case
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        schemaWithVersion = integerGroup.getLatestSchema().join();
        assertEquals(versionInfo, schemaWithVersion.getVersion());
        assertEquals("anygroup", schemaWithVersion.getSchema().getName());

        // objectTypeName
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key1", "value1"));
        integerGroup.addSchemaToGroup(schemaInfo1, groupProperties, eTag).join();
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo1).join();
        // null
        schemaWithVersion = integerGroup.getLatestSchema("anygroup2").join();
        assertEquals(null, schemaWithVersion);
        // non-null
        schemaWithVersion = integerGroup.getLatestSchema("anygroup1").join();
        assertEquals(versionInfo1, schemaWithVersion.getVersion());
        schemaWithVersion = integerGroup.getLatestSchema("anygroup").join();
        assertEquals(schemaInfo.getSchemaData(), schemaWithVersion.getSchema().getSchemaData());
    }

    @Test
    public void testGetCodecTypes() {
        // null
        List<CodecType> codecTypeList = integerGroup.getCodecTypes().join();
        assertEquals(1, codecTypeList.size());
        assertEquals(CodecType.None, codecTypeList.get(0));
        // non-null
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addCodec(CodecType.Snappy).join();
        integerGroup.addCodec(CodecType.GZip).join();
        codecTypeList = integerGroup.getCodecTypes().join();
        List<TableRecords.TableKey> codecsListKey = integerGroupTable.table.entrySet().stream().filter(
                x -> x.getKey() instanceof TableRecords.CodecsKey).map(x -> x.getKey()).collect(Collectors.toList());
        assertEquals(1, codecsListKey.size());
        TableRecords.CodecsListValue codecsListValue = integerGroupTable.getEntry(codecsListKey.get(0),
                TableRecords.CodecsListValue.class).join();
        assertEquals(2, codecsListValue.getCodecs().size());
        assertEquals(CodecType.Snappy, codecsListValue.getCodecs().get(0));
        assertEquals(2, codecTypeList.size());
        assertEquals(codecsListValue.getCodecs(), codecTypeList);
    }

    @Test
    public void testGetHistory() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key1", "value1"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        List<SchemaEvolution> schemaEvolutionList = integerGroup.getHistory().join();
        assertEquals(2, schemaEvolutionList.size());
        assertEquals("anygroup", schemaEvolutionList.get(0).getSchema().getName());
        assertEquals("anygroup1", schemaEvolutionList.get(1).getSchema().getName());
        // objectType
        byte[] schemaData1 = new byte[10];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData1,
                Collections.singletonMap("key1", "value1"));
        eTag = integerGroup.getCurrentEtag().join();
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        schemaEvolutionList = integerGroup.getHistory("anygroup1").join();
        assertEquals(2, schemaEvolutionList.size());
        assertEquals(schemaData, schemaEvolutionList.get(0).getSchema().getSchemaData());
        assertEquals(schemaData1, schemaEvolutionList.get(1).getSchema().getSchemaData());
    }

    @Test
    public void testUpdateValidationPolicy() {
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        integerGroup.updateValidationPolicy(SchemaValidationRules.of(Compatibility.forward()), eTag).join();
        List<TableRecords.TableValue> validationRecord = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.ValidationPolicyKey).map(x -> x.getValue().getValue()).collect(Collectors.toList());
        assertEquals(1, validationRecord.size());
        TableRecords.ValidationRecord validationRecord1 = (TableRecords.ValidationRecord) validationRecord.get(0);
        assertEquals(SchemaValidationRules.of(Compatibility.forward()), validationRecord1.getValidationRules());
        // when unchanged
        eTag = integerGroup.getCurrentEtag().join();
        assertEquals(null, integerGroup.updateValidationPolicy(SchemaValidationRules.of(Compatibility.forward()), eTag).join());
    }

    @Test
    public void testGetGroupProperties(){
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        GroupProperties groupProperties = integerGroup.getGroupProperties().join();
        assertEquals(SchemaType.Protobuf, groupProperties.getSchemaType());
        assertEquals(SchemaValidationRules.of(Compatibility.backward()), groupProperties.getSchemaValidationRules());
        assertTrue(groupProperties.isVersionBySchemaName());
        assertEquals(Collections.singletonMap("key", "value"), groupProperties.getProperties());
    }

    @Test
    public void testGetEncodingId(){
        //null
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward())).join();
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag).join();
        VersionInfo versionInfo = integerGroup.getVersion(schemaInfo).join();
        integerGroup.addCodec(CodecType.GZip).join();
        Either<EncodingId, Etag> idEtagEither = integerGroup.getEncodingId(versionInfo, CodecType.GZip).join();
        assertTrue(idEtagEither.isRight());
        //non-null
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId = integerGroup.createEncodingId(versionInfo, CodecType.GZip, eTag).join();
        integerGroup.addCodec(CodecType.Snappy).join();
        eTag = integerGroup.getCurrentEtag().join();
        schemaData = new byte[5];
        schemaInfo = new SchemaInfo("anygroup1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key1", "value1"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag);
        VersionInfo versionInfo1 = integerGroup.getVersion(schemaInfo).join();
        eTag = integerGroup.getCurrentEtag().join();
        EncodingId encodingId1 = integerGroup.createEncodingId(versionInfo1, CodecType.Snappy, eTag).join();
        idEtagEither = integerGroup.getEncodingId(versionInfo, CodecType.GZip).join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId, idEtagEither.getLeft());
        idEtagEither = integerGroup.getEncodingId(versionInfo1, CodecType.Snappy).join();
        assertTrue(idEtagEither.isLeft());
        assertEquals(encodingId1, idEtagEither.getLeft());
    }
}