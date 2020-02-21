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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.records.PravegaPosition;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;
import lombok.SneakyThrows;
import lombok.Synchronized;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaLog implements Log {
    private final LogCache logCache;
    private final Function<Revision, LogCache.RecordCacheKey> cacheKeySupplier;
    private final ScheduledExecutorService executorService;
    private final LogCache.ClientCacheKey clientCacheKey;
    public PravegaLog(String namespace, String group, LogCache logCache, ScheduledExecutorService executorService) {
        this.logCache = logCache;
        this.executorService = executorService;
        this.clientCacheKey = new LogCache.ClientCacheKey(namespace, group);
        this.cacheKeySupplier = revision -> new LogCache.RecordCacheKey(clientCacheKey, revision);
    }
    
    @Override
    @Synchronized
    public CompletableFuture<Position> getCurrentEtag() {
        RevisionedStreamClient<Record> client = logCache.getClient(clientCacheKey);
        return CompletableFuture.supplyAsync(() -> new PravegaPosition(client.fetchLatestRevision()), executorService);
    }

    @Override
    @Synchronized
    public CompletableFuture<Position> writeToLog(Record record, Position position) {
        RevisionedStreamClient<Record> client = logCache.getClient(clientCacheKey);

        PravegaPosition pos = position == null ? new PravegaPosition(client.fetchOldestRevision()) : (PravegaPosition) position;
        
        return CompletableFuture.supplyAsync(() -> {
                    Revision rev = client.writeConditionally(pos.getRevision(), record);
                    logCache.putRecord(cacheKeySupplier.apply(rev), record);
                    return new PravegaPosition(rev);
                }, executorService);
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    @SneakyThrows
    public <T extends Record> CompletableFuture<T> readAt(Position position, Class<T> tClass) {
        Preconditions.checkNotNull(position);
        PravegaPosition pos = (PravegaPosition) position;
        // check in the cache if the record exists
        return logCache.getRecord(cacheKeySupplier.apply(pos.getRevision()))
                .thenApply(record -> {
                    if (record.getClass().isAssignableFrom(tClass)) {
                        return (T) record;
                    } else {
                        throw new IllegalArgumentException();
                    }
                });
    }

    @Override
    @Synchronized
    public CompletableFuture<List<RecordWithPosition>> readFrom(Position position) {
        RevisionedStreamClient<Record> client = logCache.getClient(clientCacheKey);

        PravegaPosition pos = position == null ? new PravegaPosition(client.fetchOldestRevision()) : (PravegaPosition) position;

        return CompletableFuture.supplyAsync(() -> {
            Iterator<Map.Entry<Revision, Record>> records = client.readFrom(pos.getPosition());
            List<RecordWithPosition> recordWithPositions = new ArrayList<>();
            while (records.hasNext()) {
                Map.Entry<Revision, Record> entry = records.next();
                logCache.putRecord(cacheKeySupplier.apply(entry.getKey()), entry.getValue());
                recordWithPositions.add(new RecordWithPosition(new PravegaPosition(entry.getKey()), entry.getValue()));
            }
            return recordWithPositions;
        });
    }
}

