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

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.IndexRecord;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryKeyValue implements KeyValue {
    @GuardedBy("$lock")
    @Getter(AccessLevel.NONE)
    private final Map<IndexRecord.IndexKey, Value<IndexRecord.IndexValue>> index = new HashMap<>();

    @Override
    @Synchronized
    public CompletableFuture<Collection<IndexRecord.IndexKey>> getAllKeys() {
        return CompletableFuture.completedFuture(index.keySet());
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries() {
        return CompletableFuture.completedFuture(
                index.entrySet().stream().map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<List<Entry>> getAllEntries(Predicate<IndexRecord.IndexKey> filterKeys) {
        return CompletableFuture.completedFuture(index.entrySet().stream().filter(x -> filterKeys.test(x.getKey()))
                    .map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> addEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value) {
        index.putIfAbsent(key, new Value<>(value, 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> updateEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value, int version) {
        int currentVersion = index.containsKey(key) ? index.get(key).getVersion() : 0;
        if (currentVersion != version) {
            throw new StoreExceptions.WriteConflictException();
        } else {
            index.put(key, new Value<>(value, version + 1));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends IndexRecord.IndexValue> CompletableFuture<T> getRecord(IndexRecord.IndexKey key, Class<T> tClass) {
        if (!index.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value<IndexRecord.IndexValue> value = index.get(key);
        if (value.getValue().getClass().isAssignableFrom(tClass)) {
            return CompletableFuture.completedFuture((T) value.getValue());
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends IndexRecord.IndexValue> CompletableFuture<Value<T>> getRecordWithVersion(IndexRecord.IndexKey key, Class<T> tClass) {
        if (!index.containsKey(key)) {
            return CompletableFuture.completedFuture(null);
        }

        Value value = index.get(key);
        if (value.getValue().getClass().isAssignableFrom(tClass)) {
            return CompletableFuture.completedFuture(new Value<>((T) value.getValue(), value.getVersion()));
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }
}
