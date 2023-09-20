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

import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableValue;

/**
 * In memory implementation of table. 
 */
public class InMemoryGroupTable implements GroupTable<Integer> {
    @GuardedBy("$lock")
    @Getter(AccessLevel.PACKAGE)
    private final Map<TableKey, Value<TableValue, Integer>> table = new HashMap<>();

    @Override
    @Synchronized
    public CompletableFuture<List<TableKey>> getAllKeys() {
        return CompletableFuture.completedFuture(Lists.newArrayList(table.keySet()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry<Integer>>> getAllEntries() {
        return CompletableFuture.completedFuture(
                table.entrySet().stream().map(x -> new Entry<>(x.getKey(), x.getValue().getValue(), x.getValue().getVersion())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry<Integer>>> getAllEntries(Predicate<TableKey> filterKeys) {
        return CompletableFuture.completedFuture(table.entrySet().stream().filter(x -> filterKeys.test(x.getKey()))
                                                      .map(x -> new Entry<>(x.getKey(), x.getValue().getValue(), x.getValue().getVersion()))
                                                      .collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> addEntry(TableKey key, TableValue value) {
        table.putIfAbsent(key, new Value<>(value, 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> updateEntry(TableKey key, TableValue value, Integer version) {
        int currentVersion = table.containsKey(key) ? table.get(key).getVersion() : 0;
        if (version != null && currentVersion != version) {
            throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, key.getClass().toString());
        } else {
            int nextVersion = version == null ? 0 : version + 1;
            table.put(key, new Value<>(value, nextVersion));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> updateEntries(List<Entry<Integer>> updates) {
        CompletableFuture<Void> ret = new CompletableFuture<>();

        boolean isValid = updates.stream().allMatch(update -> {
            TableKey key = update.getKey();
            Integer version = update.getVersion();
            Value<TableValue, Integer> val = table.get(key);
            return version == null || val != null && version.equals(val.getVersion());
        });

        if (isValid) {
            updates.forEach(update -> updateEntry(update.getKey(), update.getValue(), update.getVersion()));
            ret.complete(null);
        } else {
            ret.completeExceptionally(StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "key"));
        }
        return ret;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableValue> CompletableFuture<T> getEntry(TableKey key, Class<T> tClass) {
        return getEntryWithVersion(key, tClass).thenApply(Value::getValue);
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableValue> CompletableFuture<Value<T, Integer>> getEntryWithVersion(TableKey key, Class<T> tClass) {
        if (!table.containsKey(key)) {
            return CompletableFuture.completedFuture(new Value<>(null, null));
        }

        Value<? extends TableValue, Integer> value = table.get(key);
        if (tClass.isAssignableFrom(value.getValue().getClass())) {
            return CompletableFuture.completedFuture(new Value<>((T) value.getValue(), value.getVersion()));
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Synchronized
    @Override
    public <T extends TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableKey> keys, Class<T> tClass) {
        return Futures.allOfWithResults(keys.stream().map(x -> getEntry(x, tClass)).collect(Collectors.toList()));
    }

    @Synchronized
    @Override
    public <T extends TableValue> CompletableFuture<List<Value<T, Integer>>> getEntriesWithVersion(List<? extends TableKey> keys, Class<T> tClass) {
        return Futures.allOfWithResults(keys.stream().map(x -> getEntryWithVersion(x, tClass)).collect(Collectors.toList()));
    }

    @Override
    public Etag toEtag(Integer version) {
        return () -> version;
    }

    @Override
    public Integer fromEtag(Etag etag) {
        return (Integer) etag.etag();
    }
}
