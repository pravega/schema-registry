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

import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Table for storing group metadata. 
 * @param <V> Version Type
 */
public interface GroupTable<V> {
    CompletableFuture<Void> addEntry(TableRecords.TableKey key, TableRecords.TableValue value);

    CompletableFuture<Void> updateEntry(TableRecords.TableKey key, TableRecords.TableValue value, V version);

    CompletableFuture<Void> updateEntries(List<Map.Entry<TableRecords.TableKey, Value<TableRecords.TableValue, V>>> entries);

    <T extends TableRecords.TableValue> CompletableFuture<T> getEntry(TableRecords.TableKey key, Class<T> tClass);

    <T extends TableRecords.TableValue> CompletableFuture<Value<T, V>> getEntryWithVersion(TableRecords.TableKey key, Class<T> tClass);

    <T extends TableRecords.TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableRecords.TableKey> keys, Class<T> tClass);

    CompletableFuture<List<TableRecords.TableKey>> getAllKeys();

    CompletableFuture<List<Entry>> getAllEntries();

    CompletableFuture<List<Entry>> getAllEntries(Predicate<TableRecords.TableKey> filterKeys);

    Etag toEtag(V version);

    V fromEtag(Etag etag);

    @Data
    class Value<T extends TableRecords.TableValue, V> {
        private final T value;
        private final V version;
    }

    @Data
    class Entry {
        private final TableRecords.TableKey key;
        private final TableRecords.TableValue value;
    }
}
