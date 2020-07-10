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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.client.VersionedRecord;
import io.pravega.schemaregistry.storage.impl.group.records.TableKeySerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.fromBytes;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaIdKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.VersionDeletedRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.EncodingIdRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.EncodingInfoRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.GroupPropertyKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.IndexTypeVersionToIdKey;

/**
 * Pravega tables based index implementation.
 */
public class PravegaKVGroupTable implements GroupTable<Version> {
    @VisibleForTesting
    static final String TABLE_NAME_FORMAT = TableStore.SCHEMA_REGISTRY_SCOPE + "/%s.#.metadata/0";
    private static final TableKeySerializer KEY_SERIALIZER = new TableKeySerializer();
    // for immutable keys check in the local cache. If its not in the cache, fetch it from the store and load it 
    // in the cache. 
    private static final List<Class<? extends TableKey>> IMMUTABLE_RECORDS =
            Lists.newArrayList(SchemaIdKey.class, VersionDeletedRecord.class, IndexTypeVersionToIdKey.class,
                    GroupPropertyKey.class, EncodingIdRecord.class, EncodingInfoRecord.class);

    private final TableStore tablesStore;
    private final String tableName;

    public PravegaKVGroupTable(String id, TableStore tablesStore) {
        this.tablesStore = tablesStore;
        this.tableName = getTableName(id);
    }

    private static String getTableName(String id) {
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
    public CompletableFuture<List<TableKey>> getAllKeys() {
        return tablesStore.getAllKeys(tableName, KEY_SERIALIZER::fromBytes);
    }

    @Override
    public CompletableFuture<List<Entry<Version>>> getAllEntries() {
        return getAllEntries(x -> true);
    }

    @Override
    public CompletableFuture<List<Entry<Version>>> getAllEntries(Predicate<TableKey> filterKeys) {
        return tablesStore.getAllEntries(tableName, x -> x, x -> x)
                          .thenApply(entries -> entries.stream().map(
                                  x -> {
                                      TableKey tableKey = KEY_SERIALIZER.fromBytes(x.getKey());
                                      TableValue tableValue = fromBytes(tableKey.getClass(), x.getValue().getRecord(), TableValue.class);
                                      return new Entry<>(tableKey, tableValue, x.getValue().getVersion());
                                  }).filter(x -> filterKeys.test(x.getKey())).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addEntry(TableKey key, TableValue value) {
        return tablesStore.addNewEntryIfAbsent(tableName, KEY_SERIALIZER.toBytes(key), value.toBytes());
    }

    @Override
    public CompletableFuture<Void> updateEntry(TableKey key, TableValue value, Version version) {
        byte[] keyBytes = KEY_SERIALIZER.toBytes(key);
        return Futures.toVoid(tablesStore.updateEntry(tableName, keyBytes, value.toBytes(), version));
    }

    @Override
    public CompletableFuture<Void> updateEntries(List<Entry<Version>> entries) {
        Map<byte[], VersionedRecord<byte[]>> batch =
                entries.stream().collect(Collectors.toMap(x -> KEY_SERIALIZER.toBytes(x.getKey()), x -> {
                    TableValue value = x.getValue();
                    return new VersionedRecord<>(value.toBytes(), x.getVersion());
                }));
        return Futures.toVoid(tablesStore.updateEntries(tableName, batch));
    }

    @Override
    public <T extends TableValue> CompletableFuture<T> getEntry(TableKey key, Class<T> tClass) {
        return getEntryWithVersion(key, tClass)
                .thenApply(Value::getValue);
    }

    @Override
    public <T extends TableValue> CompletableFuture<Value<T, Version>> getEntryWithVersion(TableKey key, Class<T> tClass) {
        if (IMMUTABLE_RECORDS.contains(key.getClass())) {
            VersionedRecord<T> cachedValue = tablesStore.getCachedRecord(tableName, key, tClass);
            if (cachedValue != null) {
                return CompletableFuture.completedFuture(new Value<>(cachedValue.getRecord(), cachedValue.getVersion()));
            }
        }
        return Futures.exceptionallyExpecting(
                tablesStore.getEntry(tableName, KEY_SERIALIZER.toBytes(key), x -> fromBytes(key.getClass(), x, tClass))
                           .thenApply(entry -> {
                               T typedRecord = getTypedRecord(entry.getRecord());
                               if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                                   tablesStore.cacheRecord(tableName, key, new VersionedRecord<>(typedRecord, entry.getVersion()));
                               }
                               return new Value<>(typedRecord, entry.getVersion());
                           }),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                new Value<>(null, null));
    }

    @Override
    public <T extends TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableKey> keys, Class<T> tClass) {
        return getEntriesWithVersion(keys, tClass)
                .thenApply(entry -> entry.stream().map(Value::getValue).collect(Collectors.toList()));
    }

    @Override
    public <T extends TableValue> CompletableFuture<List<Value<T, Version>>> getEntriesWithVersion(List<? extends TableKey> keys, Class<T> tClass) {
        List<Value<T, Version>> result = new ArrayList<>(keys.size());
        
        List<TableKey> nonCachedKeys = new LinkedList<>();
        Map<TableKey, Integer> nonCachedKeysIndex = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.add(null);
            TableKey key = keys.get(i);
            if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                VersionedRecord<T> record = tablesStore.getCachedRecord(tableName, key, tClass);
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
                                  TableKey key = nonCachedKeys.get(i);
                                  int index = nonCachedKeysIndex.get(key);
                                  VersionedRecord<byte[]> versionedRecord = values.get(i);
                                  if (!versionedRecord.getVersion().equals(Version.NON_EXISTENT)) {
                                      T value = fromBytes(key.getClass(), versionedRecord.getRecord(), tClass);
                                      Version version = versionedRecord.getVersion();
                                      if (IMMUTABLE_RECORDS.contains(key.getClass())) {
                                          tablesStore.cacheRecord(tableName, key, new VersionedRecord<>(value, versionedRecord.getVersion()));
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
    private <T extends TableValue> T getTypedRecord(TableValue value) {
        return (T) value;
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
