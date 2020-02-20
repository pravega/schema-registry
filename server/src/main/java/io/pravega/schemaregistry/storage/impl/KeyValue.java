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

import io.pravega.schemaregistry.storage.records.IndexRecord;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface KeyValue {
    CompletableFuture<Collection<IndexRecord.IndexKey>> getAllKeys();

    CompletableFuture<List<Entry>> getAllEntries();

    CompletableFuture<List<Entry>> getAllEntries(Predicate<IndexRecord.IndexKey> filterKeys);

    CompletableFuture<Void> addEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value);

    CompletableFuture<Void> updateEntry(IndexRecord.IndexKey key, IndexRecord.IndexValue value, int version);

    <T extends IndexRecord.IndexValue> CompletableFuture<T> getRecord(IndexRecord.IndexKey key, Class<T> tClass);

    <T extends IndexRecord.IndexValue> CompletableFuture<Value<T>> getRecordWithVersion(IndexRecord.IndexKey key, Class<T> tClass);

    @Data
    class Value<T extends IndexRecord.IndexValue> {
        private final T value;
        private final int version;
    }

    @Data
    class Entry {
        private final IndexRecord.IndexKey key;
        private final IndexRecord.IndexValue value;
    }
}
