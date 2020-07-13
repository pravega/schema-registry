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
import lombok.Data;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.*;

/**
 * Table for storing group metadata. 
 * @param <V> Version Type
 */
public interface GroupTable<V> {
    CompletableFuture<Void> addEntry(TableKey key, TableValue value);

    CompletableFuture<Void> updateEntry(TableKey key, TableValue value, V version);

    CompletableFuture<Void> updateEntries(List<Entry<V>> entries);

    <T extends TableValue> CompletableFuture<T> getEntry(TableKey key, Class<T> tClass);

    <T extends TableValue> CompletableFuture<Value<T, V>> getEntryWithVersion(TableKey key, Class<T> tClass);

    <T extends TableValue> CompletableFuture<List<T>> getEntries(List<? extends TableKey> keys, Class<T> tClass);
    
    <T extends TableValue> CompletableFuture<List<Value<T, V>>> getEntriesWithVersion(List<? extends TableKey> keys, Class<T> tClass);

    CompletableFuture<List<TableKey>> getAllKeys();

    CompletableFuture<List<Entry<V>>> getAllEntries();

    CompletableFuture<List<Entry<V>>> getAllEntries(Predicate<TableKey> filterKeys);

    Etag toEtag(V version);

    V fromEtag(Etag etag);

    @Data
    class Value<T extends TableValue, V> {
        private final T value;
        private final V version;
    }

    @Data
    class Entry<V> {
        private final TableKey key;
        private final TableValue value;
        private final V version;
    }
}
