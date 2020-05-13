package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.GroupTable;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
        //Map<TableRecords.TableKey, GroupTable.Value<TableRecords.TableValue, Integer>> table = new
        // InMemoryGroupTable().table;
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
                        Compatibility.backward()));
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
                        Compatibility.backward()));
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
        ListWithToken<String> stringListWithToken = integerGroup.getObjectTypes().join();
        assertTrue(integerGroupTable.table.isEmpty());
        assertEquals(Collections.emptyList(), stringListWithToken.getList());
        // non-null case
        List<String> objectTypes= new ArrayList<>();
        objectTypes.add("object1");
        objectTypes.add("object2");
        TableRecords.ObjectTypesListValue objectTypesListValue = new TableRecords.ObjectTypesListValue(objectTypes);
        TableRecords.ObjectTypesKey OBJECTS_TYPE_KEY = new TableRecords.ObjectTypesKey();
        TableRecords.TableValue tableValue = new TableRecords.ObjectTypesListValue(objectTypes);
        GroupTable.Value value = new GroupTable.Value(tableValue, 0);
        integerGroupTable.table.put(OBJECTS_TYPE_KEY, value);
        stringListWithToken = integerGroup.getObjectTypes().join();
        assertFalse(integerGroupTable.table.isEmpty());
        List<TableRecords.TableValue> tableValueList = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.ObjectTypesKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueList.isEmpty());
        assertEquals(2, stringListWithToken.getList().size());
    }

    @Test
    public void testAddSchemaToGroup(){
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward()));
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf, SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE, Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("mygroup", "anyObject", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag);
        List<TableRecords.TableValue> tableValueListEtag = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.Etag).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListEtag.isEmpty());

        List<TableRecords.TableValue> tableValueListVersionInfo = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.VersionKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListVersionInfo.isEmpty());
        assertTrue(tableValueListVersionInfo.get(0) instanceof TableRecords.SchemaRecord);
        TableRecords.SchemaRecord schemaRecord = (TableRecords.SchemaRecord) tableValueListVersionInfo.get(0);
        assertEquals("mygroup", schemaRecord.getSchemaInfo().getName());
        assertEquals(0, schemaRecord.getVersionInfo().getOrdinal());

        List<TableRecords.TableValue> tableValueListSchemaInfo = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.SchemaInfoKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListSchemaInfo.isEmpty());
        assertTrue(tableValueListSchemaInfo.get(0) instanceof TableRecords.SchemaVersionValue);
        TableRecords.SchemaVersionValue schemaVersionValue = (TableRecords.SchemaVersionValue) tableValueListSchemaInfo.get(0);
        assertEquals("mygroup", schemaVersionValue.getVersions().get(0).getObjectType());
        assertEquals(0, schemaVersionValue.getVersions().get(0).getVersion());

        List<TableRecords.TableValue> tableValueListLatestSchema = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.LatestSchemaVersionForObjectTypeKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListLatestSchema.isEmpty());
        assertTrue(tableValueListLatestSchema.get(0) instanceof TableRecords.LatestSchemaVersionValue);
        TableRecords.LatestSchemaVersionValue latestSchemaVersionValue = (TableRecords.LatestSchemaVersionValue) tableValueListLatestSchema.get(0);
        assertEquals("mygroup", latestSchemaVersionValue.getVersion().getObjectType());
        assertEquals(0, latestSchemaVersionValue.getVersion().getVersion());

        List<TableRecords.TableValue> tableValueListObjectTypesList = integerGroupTable.table.entrySet().stream().filter(x -> x.getKey() instanceof TableRecords.ObjectTypesKey).map(
                x -> x.getValue().getValue()).collect(
                Collectors.toList());
        assertFalse(tableValueListObjectTypesList.isEmpty());
        assertTrue(tableValueListObjectTypesList.get(0) instanceof TableRecords.ObjectTypesListValue);
        TableRecords.ObjectTypesListValue objectTypesListValue = (TableRecords.ObjectTypesListValue) tableValueListObjectTypesList.get(0);
        assertEquals(1, tableValueListObjectTypesList.size());
        assertEquals("mygroup", objectTypesListValue.getObjectTypes().get(0));
    }

    @Test
    public void testGetSchemas(){
        integerGroup.create(SchemaType.Protobuf, Collections.singletonMap("key", "value"), Boolean.TRUE,
                SchemaValidationRules.of(
                        Compatibility.backward()));
        Etag eTag = integerGroup.getCurrentEtag().join();
        GroupProperties groupProperties = new GroupProperties(SchemaType.Protobuf, SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE, Collections.singletonMap("key", "value"));
        byte[] schemaData = new byte[0];
        SchemaInfo schemaInfo = new SchemaInfo("anygroup", "anyObject", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        SchemaInfo schemaInfo1 = new SchemaInfo("anygroup1", "anyObject1", SchemaType.Protobuf, schemaData,
                Collections.singletonMap("key", "value"));
        integerGroup.addSchemaToGroup(schemaInfo, groupProperties, eTag);
        integerGroup.addSchemaToGroup(schemaInfo1, groupProperties, eTag);
        ListWithToken<SchemaWithVersion> schemaWithVersionListWithToken = integerGroup.getSchemas().join();
        assertEquals(2, schemaWithVersionListWithToken.getList().size());
        assertEquals(SchemaType.Protobuf, schemaWithVersionListWithToken.getList().get(0).getSchema().getSchemaType());
        // with ObjectTypeName
        schemaWithVersionListWithToken = integerGroup.getSchemas("anygroup").join();
        assertEquals(1, schemaWithVersionListWithToken.getList().size());
    }
}
