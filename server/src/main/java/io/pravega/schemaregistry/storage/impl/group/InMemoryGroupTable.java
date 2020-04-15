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
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
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

/**
 * In memory implementation of table. 
 */
public class InMemoryGroupTable implements GroupTable<Integer> {
    @GuardedBy("$lock")
    @Getter(AccessLevel.NONE)
    private final Map<TableRecords.TableKey, Value<TableRecords.TableValue, Integer>> table = new HashMap<>();

    @Override
    @Synchronized
    public CompletableFuture<List<TableRecords.TableKey>> getAllKeys() {
        return CompletableFuture.completedFuture(Lists.newArrayList(table.keySet()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries() {
        return CompletableFuture.completedFuture(
                table.entrySet().stream().map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.TableKey> filterKeys) {
        return CompletableFuture.completedFuture(table.entrySet().stream().filter(x -> filterKeys.test(x.getKey()))
                                                      .map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> addEntry(TableRecords.TableKey key, TableRecords.TableValue value) {
        table.putIfAbsent(key, new Value<>(value, 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> updateEntry(TableRecords.TableKey key, TableRecords.TableValue value, Integer version) {
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
    public CompletableFuture<Void> updateEntries(List<Map.Entry<TableRecords.TableKey,
            Value<TableRecords.TableValue, Integer>>> updates) {
        CompletableFuture<Void> ret = new CompletableFuture<>();

        boolean isValid = updates.stream().allMatch(update -> {
            TableRecords.TableKey key = update.getKey();
            Integer version = update.getValue().getVersion();
            Value<TableRecords.TableValue, Integer> val = table.get(key);
            return version == null || (val != null && version.equals(val.getVersion()));
        });

        if (isValid) {
            updates.forEach(update -> updateEntry(update.getKey(), update.getValue().getValue(), update.getValue().getVersion()));
            ret.complete(null);
        } else {
            ret.completeExceptionally(StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "key"));
        }
        return ret;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableRecords.TableValue> CompletableFuture<T> getEntry(TableRecords.TableKey key, Class<T> tClass) {
        if (!table.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value<TableRecords.TableValue, Integer> value = table.get(key);
        if (tClass.isAssignableFrom(value.getValue().getClass())) {
            return CompletableFuture.completedFuture((T) value.getValue());
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Synchronized
    @Override
    public <T extends TableRecords.TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableRecords.TableKey> keys, Class<T> tClass) {
        return Futures.allOfWithResults(keys.stream().map(x -> getEntry(x, tClass)).collect(Collectors.toList()));
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends TableRecords.TableValue> CompletableFuture<Value<T, Integer>> getEntryWithVersion(TableRecords.TableKey key, Class<T> tClass) {
        if (!table.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value<? extends TableRecords.TableValue, Integer> value = table.get(key);
        if (tClass.isAssignableFrom(value.getValue().getClass())) {
            return CompletableFuture.completedFuture(new Value<>((T) value.getValue(), value.getVersion()));
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
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
