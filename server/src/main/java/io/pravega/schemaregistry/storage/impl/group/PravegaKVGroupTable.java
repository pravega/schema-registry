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

import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.TableKeySerializer;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Pravega tables based index implementation.
 */
public class PravegaKVGroupTable implements GroupTable<Version> {
    private static final String TABLE_NAME_FORMAT = "table-%s/metadata/0";
    private static final TableKeySerializer KEY_SERIALIZER = new TableKeySerializer();
    // for immutable keys check in the local cache. If its not in the cache, fetch it from the store and load it 
    // in the cache. 
    private static final List<Class<? extends TableRecords.TableKey>> IMMUTABLE_RECORDS =
            Lists.newArrayList(TableRecords.VersionKey.class, TableRecords.VersionDeletedRecord.class, 
                    TableRecords.GroupPropertyKey.class, TableRecords.EncodingIdRecord.class, TableRecords.EncodingInfoRecord.class);

    final TableStore tablesStore;
    private final String tableName;

    public PravegaKVGroupTable(String groupName, String id, TableStore tablesStore) {
        this.tablesStore = tablesStore;
        this.tableName = getTableName(groupName, id);
    }

    private static String getTableName(String groupName, String id) {
        return String.format(TABLE_NAME_FORMAT, id);
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
    public CompletableFuture<List<TableRecords.TableKey>> getAllKeys() {
        return tablesStore.getAllKeys(tableName, KEY_SERIALIZER::fromBytes);
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries() {
        return getAllEntries(x -> true);
    }

    @Override
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.TableKey> filterKeys) {
        return tablesStore.getAllEntries(tableName, x -> x, x -> x)
                          .thenApply(entries -> entries.stream().map(
                                  x -> {
                                      TableRecords.TableKey tableKey = KEY_SERIALIZER.fromBytes(x.getKey());
                                      TableRecords.TableValue tableValue = TableRecords.fromBytes(tableKey.getClass(), x.getValue().getRecord(), TableRecords.TableValue.class);
                                      return new Entry(tableKey, tableValue);
                                  }).filter(x -> filterKeys.test(x.getKey())).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addEntry(TableRecords.TableKey key, TableRecords.TableValue value) {
        return tablesStore.addNewEntryIfAbsent(tableName, KEY_SERIALIZER.toBytes(key), value.toBytes());
    }

    @Override
    public CompletableFuture<Void> updateEntry(TableRecords.TableKey key, TableRecords.TableValue value, Version version) {
        byte[] keyBytes = KEY_SERIALIZER.toBytes(key);
        return Futures.toVoid(tablesStore.updateEntry(tableName, keyBytes, value.toBytes(), version));
    }

    @Override
    public CompletableFuture<Void> updateEntries(List<Map.Entry<TableRecords.TableKey, Value<TableRecords.TableValue, Version>>> entries) {
        Map<byte[], Map.Entry<byte[], Version>> batch =
                entries.stream().collect(Collectors.toMap(x -> KEY_SERIALIZER.toBytes(x.getKey()), x -> {
                    Value<TableRecords.TableValue, Version> valueWithVersion = x.getValue();
                    return new AbstractMap.SimpleEntry<>(valueWithVersion.getValue().toBytes(), valueWithVersion.getVersion());
                }));
        return Futures.toVoid(tablesStore.updateEntries(tableName, batch));
    }

    @Override
    public <T extends TableRecords.TableValue> CompletableFuture<T> getEntry(TableRecords.TableKey key, Class<T> tClass) {
        return getEntryWithVersion(key, tClass)
                .thenApply(Value::getValue);
    }

    @Override
    public <T extends TableRecords.TableValue> CompletableFuture<Value<T, Version>> getEntryWithVersion(TableRecords.TableKey key, Class<T> tClass) {
        if (IMMUTABLE_RECORDS.contains(key.getClass())) {
            Version.VersionedRecord<T> cachedValue = tablesStore.getCachedRecord(tableName, key, tClass);
            if (cachedValue != null) {
                return CompletableFuture.completedFuture(new Value<>(cachedValue.getRecord(), cachedValue.getVersion()));
            }
        }
        return Futures.exceptionallyExpecting(
                tablesStore.getEntry(tableName, KEY_SERIALIZER.toBytes(key), x -> TableRecords.fromBytes(key.getClass(), x, tClass))
                           .thenApply(entry -> {
                               T typedRecord = getTypedRecord(tClass, entry.getRecord());
                               if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                                   tablesStore.cacheRecord(tableName, key, new Version.VersionedRecord<>(typedRecord, entry.getVersion()));
                               }
                               return new Value<>(typedRecord, entry.getVersion());
                           }),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                new Value<>(null, null));
    }

    @Override
    public <T extends TableRecords.TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableRecords.TableKey> keys, Class<T> tClass) {
        return getEntriesWithVersion(keys, tClass)
                .thenApply(entry -> entry.stream().map(Value::getValue).collect(Collectors.toList()));
    }

    @Override
    public <T extends TableRecords.TableValue> CompletableFuture<List<Value<T, Version>>> getEntriesWithVersion(List<? extends TableRecords.TableKey> keys, Class<T> tClass) {
        List<Value<T, Version>> result = new ArrayList<>(keys.size());
        
        List<TableRecords.TableKey> nonCachedKeys = new LinkedList<>();
        Map<TableRecords.TableKey, Integer> nonCachedKeysIndex = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.add(null);
            TableRecords.TableKey key = keys.get(i);
            if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                Version.VersionedRecord<T> record = tablesStore.getCachedRecord(tableName, key, tClass);
                if (record != null) {
                    result.set(i, new Value<>(record.getRecord(), record.getVersion()));
                } 
            } 
            if (result.get(i) == null) {
                nonCachedKeysIndex.put(key, i);
                nonCachedKeys.add(key);
            }
        }
        return tablesStore.getEntries(tableName,
                nonCachedKeys.stream().map(KEY_SERIALIZER::toBytes).collect(Collectors.toList()), false)
                          .thenApply(values -> {
                              for (int i = 0; i < nonCachedKeys.size(); i++) {
                                  TableRecords.TableKey key = nonCachedKeys.get(i);
                                  int index = nonCachedKeysIndex.get(key);
                                  Version.VersionedRecord<byte[]> versionedRecord = values.get(i);
                                  if (!versionedRecord.getVersion().equals(Version.NON_EXISTENT)) {
                                      T value = TableRecords.fromBytes(key.getClass(), versionedRecord.getRecord(), tClass);
                                      Version version = versionedRecord.getVersion();
                                      if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                                          tablesStore.cacheRecord(tableName, key, new Version.VersionedRecord<>(value, versionedRecord.getVersion()));
                                      }

                                      result.set(index, new Value<>(value, version));
                                  } else {
                                      result.set(index, new Value<>(null, null));
                                  }
                              }
                              return result;
                          });
    }


    @SuppressWarnings("unchecked")
    private <T extends TableRecords.TableValue> T getTypedRecord(Class<T> tClass, TableRecords.TableValue value) {
        if (tClass.isAssignableFrom(value.getClass())) {
            return (T) value;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public Etag<Version> toEtag(Version version) {
        return () -> version;
    }

    @Override
    public Version fromEtag(Etag etag) {
        return (Version) etag.etag();
    }
}
