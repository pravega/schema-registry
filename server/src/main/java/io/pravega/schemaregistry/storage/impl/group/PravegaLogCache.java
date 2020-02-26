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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordSerializer;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaLogCache {
    private static final int MAX_CACHE_SIZE = 10000;
    private final LoadingCache<RecordCacheKey, CompletableFuture<Record>> recordCache;
    private final LoadingCache<ClientCacheKey, RevisionedStreamClient<Record>> clientCache;
    
    public PravegaLogCache(ClientConfig clientConfig) {
        this.clientCache =
                CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .build(new CacheLoader<ClientCacheKey, RevisionedStreamClient<Record>>() {
                                @ParametersAreNonnullByDefault
                                @Override
                                public RevisionedStreamClient<Record> load(final ClientCacheKey key) {
                                    SynchronizerClientFactory clientFactory = SynchronizerClientFactory.withScope(
                                            PravegaLog.SCHEMA_REGISTRY_SCOPE, clientConfig);
                                    String logName = PravegaLog.getLogName(key.namespace, key.group, key.id);

                                    return clientFactory.createRevisionedStreamClient(logName, new RecordsSerializer<>(),
                                            SynchronizerConfig.builder().build());
                                }
                            });
        
        this.recordCache =
                CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .build(new CacheLoader<RecordCacheKey, CompletableFuture<Record>>() {
                                @ParametersAreNonnullByDefault
                                @Override
                                @SneakyThrows
                                public CompletableFuture<Record> load(final RecordCacheKey key) {
                                    RevisionedStreamClient<Record> cl = clientCache.get(key.clientCacheKey);
                                    Iterator<Map.Entry<Revision, Record>> records = cl.readFrom(key.revision);
                                    CompletableFuture<Record> future = new CompletableFuture<>();
                                    while (records.hasNext()) {
                                        Map.Entry<Revision, Record> entry = records.next();
                                        if (!future.isDone()) {
                                            future.complete(entry.getValue());
                                        }
                                        RecordCacheKey recordCacheKey = new RecordCacheKey(key.clientCacheKey, entry.getKey());
                                        recordCache.put(recordCacheKey, CompletableFuture.completedFuture(entry.getValue()));
                                    }
                                    if (!future.isDone()) {
                                        future.completeExceptionally(new StoreExceptions.DataNotFoundException());
                                    }
                                    return future;
                                }
                            });
    }

    @SneakyThrows
    public CompletableFuture<Record> getRecord(RecordCacheKey recordCacheKey) {
        return recordCache.get(recordCacheKey);    
    }
    
    public void putRecord(RecordCacheKey key, Record record) {
        recordCache.put(key, CompletableFuture.completedFuture(record));    
    }
    
    @SneakyThrows
    public RevisionedStreamClient<Record> getClient(ClientCacheKey clientCacheKey) {
        return clientCache.get(clientCacheKey);
    }
    
    @Data
    static class RecordCacheKey {
        private final ClientCacheKey clientCacheKey;
        private final Revision revision;
    }

    @Data
    static class ClientCacheKey {
        private final String namespace;
        private final String group;
        private final String id;
    }

    static class RecordsSerializer<T extends Record> implements Serializer<T> {
        private final RecordSerializer baseSerializer;

        public RecordsSerializer() {
            this(new RecordSerializer());
        }

        @VisibleForTesting
        public RecordsSerializer(@NonNull RecordSerializer baseSerializer) {
            this.baseSerializer = baseSerializer;
        }

        @Override
        public ByteBuffer serialize(T value) {
            return this.baseSerializer.toByteBuffer(value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public T deserialize(ByteBuffer serializedValue) {
            return (T) this.baseSerializer.fromByteBuffer(serializedValue);
        }
    }
}
