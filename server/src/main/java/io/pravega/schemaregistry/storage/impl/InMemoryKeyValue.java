/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

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
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class InMemoryKeyValue implements KeyValue {
    @GuardedBy("$lock")
    @Getter(AccessLevel.NONE)
    private final Map<IndexRecord.IndexKey, Value<IndexRecord.IndexValue>> index = new HashMap<>();

    @Override
    @Synchronized
    public Collection<IndexRecord.IndexKey> getAllKeys() {
        return index.keySet();
    }

    @Override
    @Synchronized
    public List<Entry> getAllEntries() {
        return index.entrySet().stream().map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList());
    }

    @Override
    @Synchronized
    public List<Entry> getAllEntries(Predicate<IndexRecord.IndexKey> filterKeys) {
        return index.entrySet().stream().filter(x -> filterKeys.test(x.getKey()))
                    .map(x -> new Entry(x.getKey(), x.getValue().getValue())).collect(Collectors.toList());
    }

    @Override
    @Synchronized
    public void addEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value) {
        index.putIfAbsent(key, new Value<>(value, 0));
    }

    @Override
    @Synchronized
    public void updateEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value, int version) {
        int currentVersion = index.containsKey(key) ? index.get(key).getVersion() : 0;
        if (currentVersion != version) {
            throw new StoreExceptions.WriteConflictException();
        } else {
            index.put(key, new Value<>(value, version + 1));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends IndexRecord.IndexValue> T getRecord(IndexRecord.IndexKey key, Class<T> tClass) {
        if (!index.containsKey(key)) {
            return null;
        }

        Value<IndexRecord.IndexValue> value = index.get(key);
        if (value.getValue().getClass().isAssignableFrom(tClass)) {
            return (T) value.getValue();
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends IndexRecord.IndexValue> Value<T> getRecordWithVersion(IndexRecord.IndexKey key, Class<T> tClass) {
        if (!index.containsKey(key)) {
            return null;
        }

        Value value = index.get(key);
        if (value.getValue().getClass().isAssignableFrom(tClass)) {
            return new Value<>((T) value.getValue(), value.getVersion());
        } else {
            throw new IllegalArgumentException();
        }
    }
}
