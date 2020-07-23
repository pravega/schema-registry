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
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TablesStoreTest {
    private ScheduledExecutorService executor;
    private WireCommandClient wireCommandClient;
    private TableStore tableStore;

    @Before
    public void setup() throws Exception {
        executor = Executors.newScheduledThreadPool(5);
        wireCommandClient = WireCommandMock.getMock(executor);
        tableStore = new TableStore(wireCommandClient, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test
    public void testTables() {
        // create table
        String table = "schemaregistry/table";
        tableStore.createTable(table).join();
        String key = "key";
        String value = "value";
        byte[] valueBytes = value.getBytes();
        // non existent table
        AssertExtensions.assertFutureThrows("non existent table", tableStore.getEntry("a/nonExistentTable", key.getBytes(), x -> x),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException);

        tableStore.addNewEntryIfAbsent(table, key.getBytes(), valueBytes).join();

        // get entry
        VersionedRecord<String> entry = tableStore.getEntry(table, key.getBytes(), String::new).join();
        assertEquals(entry.getRecord(), value);
        
        List<String> keys = tableStore.getAllKeys(table, String::new).join();
        assertEquals(keys.size(), 1);
        assertEquals(keys.get(0), key);
        
        // get all entries
        List<VersionedEntry<String, String>> entries = tableStore.getAllEntries(table, String::new, String::new).join();

        assertEquals(entries.size(), 1);        
        assertEquals(entries.get(0).getKey(), key);        
        assertEquals(entries.get(0).getValue().getRecord(), value);        
        
        // update entry
        value = "value2";
        valueBytes = value.getBytes();
        Version version = entry.getVersion();
        tableStore.updateEntry(table, key.getBytes(), valueBytes, version).join();
        // bad version update
        AssertExtensions.assertFutureThrows("bad version", tableStore.updateEntry(table, key.getBytes(), valueBytes, version),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException);
        // get and verify
        entry = tableStore.getEntry(table, key.getBytes(), String::new).join();
        assertEquals(entry.getRecord(), value);

        // check delete non empty table
        AssertExtensions.assertFutureThrows("Not Empty", tableStore.deleteTable(table, true), 
            e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotEmptyException);
        
        // remove entry
        tableStore.removeEntry(table, key.getBytes()).join();
        AssertExtensions.assertFutureThrows("", tableStore.getEntry(table, key.getBytes(), String::new), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);
        // idempotent remove
        tableStore.removeEntry(table, key.getBytes()).join();
        
        tableStore.addNewEntryIfAbsent(table, key.getBytes(), valueBytes).join();
        entry = tableStore.getEntry(table, key.getBytes(), String::new).join();
        assertEquals(entry.getRecord(), value);
        version = entry.getVersion();
        
        // idempotent
        tableStore.addNewEntryIfAbsent(table, key.getBytes(), valueBytes).join();
        entry = tableStore.getEntry(table, key.getBytes(), String::new).join();
        assertEquals(entry.getVersion(), version);

        keys = Lists.newArrayList("1", "2", "3");
        Map<byte[], VersionedRecord<byte[]>> entriesToAdd = new HashMap<>();
        entriesToAdd.put(keys.get(0).getBytes(), new VersionedRecord<>(keys.get(0).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(1).getBytes(), new VersionedRecord<>(keys.get(1).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(2).getBytes(), new VersionedRecord<>(keys.get(2).getBytes(), Version.NON_EXISTENT));
        tableStore.updateEntries(table, entriesToAdd).join();

        // get all keys paginated
        ByteBuf token = Unpooled.wrappedBuffer(Base64.getDecoder().decode(""));
        ResultPage<String, ByteBuf> response = tableStore.getKeysPaginated(table, token, 2, String::new).join();
        assertEquals(response.getList().size(), 2);
        assertTrue(response.getToken().hasArray());

        response = tableStore.getKeysPaginated(table, response.getToken(), 2, String::new).join();
        assertEquals(response.getList().size(), 2);
        assertTrue(response.getToken().hasArray());

        // remove entries
        tableStore.removeEntry(table, key.getBytes()).join();

        // non existent key
        AssertExtensions.assertFutureThrows("non existent key", tableStore.getEntry(table, "nonExistentKey".getBytes(), x -> x),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException);

        keys = Lists.newArrayList("4", "5", "non existent", "7");
        entriesToAdd = new HashMap<>();
        entriesToAdd.put(keys.get(0).getBytes(), new VersionedRecord<>(keys.get(0).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(1).getBytes(), new VersionedRecord<>(keys.get(1).getBytes(), Version.NON_EXISTENT));
        entriesToAdd.put(keys.get(3).getBytes(), new VersionedRecord<>(keys.get(3).getBytes(), Version.NON_EXISTENT));
        tableStore.updateEntries(table, entriesToAdd).join();

        List<VersionedRecord<byte[]>> values = tableStore.getEntries(table, keys.stream().map(String::getBytes).collect(Collectors.toList()),
                false).join();
        assertEquals(keys.size(), values.size());
        assertEquals(keys.get(0), new String(values.get(0).getRecord()));
        assertEquals(keys.get(1), new String(values.get(1).getRecord()));
        assertSame(values.get(2).getVersion().toLong(), Version.NON_EXISTENT.toLong());
        assertEquals(keys.get(3), new String(values.get(3).getRecord()));
    }
}
