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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.PravegaPosition;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;
import lombok.SneakyThrows;
import lombok.Synchronized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Pravega revisioned stream based log implementation. 
 */
public class PravegaLog implements Log {
    private static final String LOG_SCOPE_FORMAT = "scope-%s";
    private static final String LOG_NAME_FORMAT = "log-%s";
    
    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    private final PravegaLogCache.ClientCacheKey clientCacheKey;
    private final Function<Revision, PravegaLogCache.RecordCacheKey> cacheKeySupplier;
    private final ScheduledExecutorService executorService;
    private final String scopeName;
    private final String logName;
    
    public PravegaLog(String group, String id, ClientConfig clientConfig,
                      PravegaLogCache logCache, ScheduledExecutorService executorService) {
        this.logCache = logCache;
        this.clientConfig = clientConfig;
        this.executorService = executorService;
        this.clientCacheKey = new PravegaLogCache.ClientCacheKey(group, id);
        this.cacheKeySupplier = revision -> new PravegaLogCache.RecordCacheKey(clientCacheKey, revision);
        this.scopeName = getScopeName(group, id);
        this.logName = getLogName(group, id);
    }

    static String getScopeName(String group, String id) {
        return String.format(LOG_SCOPE_FORMAT, id);
    }

    static String getLogName(String group, String id) {
        return String.format(LOG_NAME_FORMAT, id);
    }
    
    public CompletableFuture<Void> create() {
        return CompletableFuture.runAsync(() -> {
            StreamManager manager = new StreamManagerImpl(clientConfig);
            manager.createScope(scopeName);
            manager.createStream(scopeName, logName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }, executorService);
    }

    public CompletableFuture<Void> delete() {
        return CompletableFuture.runAsync(() -> {
            StreamManager manager = new StreamManagerImpl(clientConfig);
            manager.deleteStream(logName, logName);
        }, executorService);
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
                    if (rev == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, pos.getRevision().toString());
                    }
                    logCache.putRecord(cacheKeySupplier.apply(pos.getRevision()), new PravegaLogCache.RecordCacheValue(record, rev));
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
        return CompletableFuture.supplyAsync(() -> {
            PravegaLogCache.RecordCacheValue recordCacheValue = logCache.getRecord(cacheKeySupplier.apply(pos.getRevision()));
            if (tClass.isAssignableFrom(recordCacheValue.getRecord().getClass())) {
                return (T) recordCacheValue.getRecord();
            } else {
                throw new IllegalArgumentException();
            }
        }, executorService);
    }

    @Override
    @Synchronized
    public CompletableFuture<List<RecordWithPosition>> readFrom(Position position) {
        RevisionedStreamClient<Record> client = logCache.getClient(clientCacheKey);

        PravegaPosition pos = position == null ? new PravegaPosition(client.fetchOldestRevision()) : (PravegaPosition) position;
        
        return getCurrentEtag().thenApplyAsync(latest -> {
            Revision current = pos.getRevision();
            List<RecordWithPosition> recordWithPositions = new ArrayList<>();

            while (!current.equals(latest.getPosition())) {
                PravegaLogCache.RecordCacheValue value = logCache.getRecord(cacheKeySupplier.apply(current));
                Revision next = value.getNextRevision();
                recordWithPositions.add(new RecordWithPosition(new PravegaPosition(current), value.getRecord(), 
                        new PravegaPosition(next)));
                current = next;
            }

            return recordWithPositions;
        }, executorService);
    }
}

