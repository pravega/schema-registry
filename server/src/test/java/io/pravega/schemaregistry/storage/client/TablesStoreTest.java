/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TablesStoreTest {
    private static final String SCHEMAREGISTRY_TABLE = "schemaregistry/table";
    private static final String KEY = "key";
    private static final byte[] KEY_BYTES = KEY.getBytes();
    private static final String VALUE = "value";
    private static final byte[] VALUE_BYTES = VALUE.getBytes();
    
    private ScheduledExecutorService executor;
    private TableStore tableStore;

    @Before
    public void setup() throws Exception {
        executor = Executors.newScheduledThreadPool(5);
        WireCommandClient wireCommandClient = WireCommandMock.getMock(executor);
        tableStore = new TableStore(wireCommandClient, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testNonExistantTable() {
        // non existent table
        AssertExtensions.assertFutureThrows("non existent table", tableStore.getEntry("a/nonExistentTable", KEY_BYTES, x -> x),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException);
    }

    @Test
    public void addAndGetEntryTest() {
        tableStore.createTable(SCHEMAREGISTRY_TABLE).join();

        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();
        VersionedRecord<String> entry = tableStore.getEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, String::new).join();
        assertEquals(entry.getRecord(), VALUE);
        Version version = entry.getVersion();

        // idempotent
        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();
        entry = tableStore.getEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, String::new).join();
        assertEquals(entry.getVersion(), version);

        // non existent key
        AssertExtensions.assertFutureThrows("non existent key", tableStore.getEntry(SCHEMAREGISTRY_TABLE, "nonExistentKey".getBytes(), x -> x),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
    }

    @Test
    public void getAllKeysAndEntriesTest() {
        tableStore.createTable(SCHEMAREGISTRY_TABLE).join();
        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();

        // get all keys
        List<String> keys = tableStore.getAllKeys(SCHEMAREGISTRY_TABLE, String::new).join();
        assertEquals(keys.size(), 1);
        assertEquals(keys.get(0), KEY);

        // get all entries
        List<VersionedEntry<String, String>> entries = tableStore.getAllEntries(SCHEMAREGISTRY_TABLE, String::new, String::new).join();
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getKey(), KEY);
        assertEquals(entries.get(0).getValue().getRecord(), VALUE);
    }

    @Test
    public void updateEntryTest() {
        tableStore.createTable(SCHEMAREGISTRY_TABLE).join();
        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();
        VersionedRecord<String> entry = tableStore.getEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, String::new).join();
        Version version = entry.getVersion();

        String value = "value2";
        byte[] update = value.getBytes();

        tableStore.updateEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, update, version).join();
        // bad version update
        AssertExtensions.assertFutureThrows("bad version", tableStore.updateEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, update, version),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException);
        entry = tableStore.getEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, String::new).join();
        assertEquals(entry.getRecord(), value);
    }

    @Test
    public void deleteEntryTest() {
        tableStore.createTable(SCHEMAREGISTRY_TABLE).join();
        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();

        // check delete non empty table
        AssertExtensions.assertFutureThrows("Not Empty", tableStore.deleteTable(SCHEMAREGISTRY_TABLE, true),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotEmptyException);
        // remove entry
        tableStore.removeEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES).join();
        AssertExtensions.assertFutureThrows("Entry should not be found", tableStore.getEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES, String::new),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);

        // idempotent remove
        tableStore.removeEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES).join();
    }
    
    @Test
    public void updateMultipleEntriesAndGetPaginatedTest() {
        tableStore.createTable(SCHEMAREGISTRY_TABLE).join();
        tableStore.addNewEntryIfAbsent(SCHEMAREGISTRY_TABLE, KEY_BYTES, VALUE_BYTES).join();

        ArrayList<String> keys = Lists.newArrayList("1", "2", "3");
        Map<byte[], VersionedRecord<byte[]>> entriesToAdd = new HashMap<>();
        entriesToAdd.put(keys.get(0).getBytes(), new VersionedRecord<>(keys.get(0).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(1).getBytes(), new VersionedRecord<>(keys.get(1).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(2).getBytes(), new VersionedRecord<>(keys.get(2).getBytes(), Version.NON_EXISTENT));
        tableStore.updateEntries(SCHEMAREGISTRY_TABLE, entriesToAdd).join();

        // get all keys paginated
        ByteBuf token = Unpooled.wrappedBuffer(Base64.getDecoder().decode(""));
        ResultPage<String, ByteBuf> response = tableStore.getKeysPaginated(SCHEMAREGISTRY_TABLE, token, 2, String::new).join();
        assertEquals(response.getList().size(), 2);
        assertTrue(response.getToken().hasArray());

        response = tableStore.getKeysPaginated(SCHEMAREGISTRY_TABLE, response.getToken(), 2, String::new).join();
        assertEquals(response.getList().size(), 2);
        assertTrue(response.getToken().hasArray());

        // remove entries
        tableStore.removeEntry(SCHEMAREGISTRY_TABLE, KEY_BYTES).join();

        keys = Lists.newArrayList("4", "5", "non existent", "7");
        entriesToAdd = new HashMap<>();
        entriesToAdd.put(keys.get(0).getBytes(), new VersionedRecord<>(keys.get(0).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(1).getBytes(), new VersionedRecord<>(keys.get(1).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(3).getBytes(), new VersionedRecord<>(keys.get(3).getBytes(), Version.NON_EXISTENT));
        tableStore.updateEntries(SCHEMAREGISTRY_TABLE, entriesToAdd).join();

        List<VersionedRecord<byte[]>> values = tableStore.getEntries(SCHEMAREGISTRY_TABLE, keys.stream().map(String::getBytes).collect(Collectors.toList()),
                false).join();
        assertEquals(keys.size(), values.size());
        assertEquals(keys.get(0), new String(values.get(0).getRecord()));
        assertEquals(keys.get(1), new String(values.get(1).getRecord()));
        assertSame(values.get(2).getVersion().toLong(), Version.NON_EXISTENT.toLong());
        assertEquals(keys.get(3), new String(values.get(3).getRecord()));
    }
}
