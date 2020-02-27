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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
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
    public static final String SCHEMA_REGISTRY_SCOPE = "pravega-schema-registry";
    public static final String LOG_NAME_FORMAT = "log-%s-%s-%s";
    
    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    private final PravegaLogCache.ClientCacheKey clientCacheKey;
    private final Function<Revision, PravegaLogCache.RecordCacheKey> cacheKeySupplier;
    private final ScheduledExecutorService executorService;
    private final String logName;
    
    public PravegaLog(String namespace, String group, String id, ClientConfig clientConfig,
                      PravegaLogCache logCache, ScheduledExecutorService executorService) {
        this.logCache = logCache;
        this.clientConfig = clientConfig;
        this.executorService = executorService;
        this.clientCacheKey = new PravegaLogCache.ClientCacheKey(namespace, group, id);
        this.cacheKeySupplier = revision -> new PravegaLogCache.RecordCacheKey(clientCacheKey, revision);
        this.logName = getLogName(namespace, group, id);
    }

    static String getLogName(String namespace, String group, String groupId) {
        return String.format(LOG_NAME_FORMAT, namespace, group, groupId);
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
                    if (tClass.isAssignableFrom(record.getClass())) {
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

