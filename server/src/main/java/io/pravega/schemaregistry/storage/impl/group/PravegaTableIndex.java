/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.namespace.PravegaTableNamespaces;
import io.pravega.schemaregistry.storage.records.IndexKeySerializer;
import io.pravega.schemaregistry.storage.records.IndexRecord;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PravegaTableIndex implements Index<Version> {
    public static final String TABLE_NAME_FORMAT = PravegaTableNamespaces.SCHEMA_REGISTRY_SCOPE + "/table-%s-%s-%s/0";
    private static final IndexKeySerializer INDEX_KEY_SERIALIZER = new IndexKeySerializer();
    
    private final TableStore tablesStore;
    private final String tableName;

    public PravegaTableIndex(String namespace, String groupName, String id, TableStore tablesStore) {
        this.tablesStore = tablesStore;
        this.tableName = getIndexName(namespace, groupName, id); 
    }

    static String getIndexName(String namespace, String groupName, String id) {
        return String.format(TABLE_NAME_FORMAT, namespace, groupName, id);
    }
    
    public CompletableFuture<Void> create() {
        // create new table
        return tablesStore.createTable(tableName);
    }
    
    public CompletableFuture<Void> delete() {
        // delete the table
        return tablesStore.deleteTable(tableName, false);
    }
    
    @Override
    public CompletableFuture<Collection<IndexRecord.IndexKey>> getAllKeys() {
        return tablesStore.getAllKeys(tableName)
                          .thenApply(list -> list.stream().map(INDEX_KEY_SERIALIZER::fromString).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries() {
        return getAllEntries(x -> true);
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<IndexRecord.IndexKey> filterKeys) {

        return tablesStore.getAllEntries(tableName, x -> x)
                          .thenApply(entries -> entries.stream().map( 
                                  x -> {
                                      IndexRecord.IndexKey indexKey = INDEX_KEY_SERIALIZER.fromString(x.getKey());
                                      IndexRecord.IndexValue indexValue = IndexRecord.fromBytes(indexKey.getClass(), x.getValue().getObject());
                                      return new Entry(indexKey, indexValue);
                                  }).filter(x -> filterKeys.test(x.getKey())).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value) {
        return Futures.toVoid(tablesStore.addNewEntryIfAbsent(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), value.toBytes()));
    }

    @Override
    public CompletableFuture<Void> updateEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value, Version version) {
        String keyStr = INDEX_KEY_SERIALIZER.toKeyString(key);
         return Futures.toVoid(tablesStore.updateEntry(tableName, keyStr, value.toBytes(), version));
    }

    @Override
    public <T extends IndexRecord.IndexValue> CompletableFuture<T> getRecord(IndexRecord.IndexKey key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(
                tablesStore.getEntry(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), x -> IndexRecord.fromBytes(key.getClass(), x))
                          .thenApply(entry -> getTypedRecord(tClass, entry.getObject())), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, 
                null);
    }

    @SuppressWarnings("unchecked")
    private <T extends IndexRecord.IndexValue> T getTypedRecord(Class<T> tClass, IndexRecord.IndexValue value) {
        if (tClass.isAssignableFrom(value.getClass())) {
            return (T) value;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public <T extends IndexRecord.IndexValue> CompletableFuture<Value<T, Version>> getRecordWithVersion(IndexRecord.IndexKey key, Class<T> tClass) {
        return Futures.exceptionallyExpecting(tablesStore.getEntry(tableName, INDEX_KEY_SERIALIZER.toKeyString(key), x -> IndexRecord.fromBytes(key.getClass(), x))
                   .thenApply(entry -> {
                       T t = getTypedRecord(tClass, entry.getObject());
                       return new Value<>(t, entry.getVersion());
                   }), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, 
                null);
    }
}
