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

public class PravegaLog implements Log {
    static final String SCHEMA_REGISTRY_SCOPE = "pravega-schema-registry";
    private static final String LOG_NAME_FORMAT = "log-%s-%s-%s";
    
    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    private final PravegaLogCache.ClientCacheKey clientCacheKey;
    private final Function<Revision, PravegaLogCache.RecordCacheKey> cacheKeySupplier;
    private final ScheduledExecutorService executorService;
    private final String logName;
    
    public PravegaLog(String group, String id, ClientConfig clientConfig,
                      PravegaLogCache logCache, ScheduledExecutorService executorService) {
        this.logCache = logCache;
        this.clientConfig = clientConfig;
        this.executorService = executorService;
        this.clientCacheKey = new PravegaLogCache.ClientCacheKey(group, id);
        this.cacheKeySupplier = revision -> new PravegaLogCache.RecordCacheKey(clientCacheKey, revision);
        this.logName = getLogName(group, id);
    }

    static String getLogName(String group, String groupId) {
        return String.format(LOG_NAME_FORMAT, group, groupId);
    }
    
    public CompletableFuture<Void> create() {
        return CompletableFuture.runAsync(() -> {
            StreamManager manager = new StreamManagerImpl(clientConfig);
            manager.createScope(SCHEMA_REGISTRY_SCOPE);
            manager.createStream(SCHEMA_REGISTRY_SCOPE, logName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        }, executorService);
    }

    public CompletableFuture<Void> delete() {
        return CompletableFuture.runAsync(() -> {
            StreamManager manager = new StreamManagerImpl(clientConfig);
            manager.deleteStream(SCHEMA_REGISTRY_SCOPE, logName);
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
                    return new PravegaPosition(pos.getRevision());
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

            PravegaLogCache.RecordCacheValue value = logCache.getRecord(cacheKeySupplier.apply(current));
            while (value != null) {
                recordWithPositions.add(new RecordWithPosition(new PravegaPosition(current), value.getRecord()));
                current = value.getNextRevision();
                value = current.equals(latest.getPosition()) ? null : logCache.getRecord(cacheKeySupplier.apply(current));
            }

            return recordWithPositions;
        }, executorService);
    }
}

