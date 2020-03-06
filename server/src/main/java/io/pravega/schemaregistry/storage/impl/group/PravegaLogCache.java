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
import com.google.common.util.concurrent.UncheckedExecutionException;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Log cache for caching entries read from {@link PravegaLog}.  
 */
public class PravegaLogCache {
    private static final int MAX_CACHE_SIZE = 10000;
    private final LoadingCache<RecordCacheKey, RecordCacheValue> recordCache;
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
                                    String logName = PravegaLog.getLogName(key.group, key.id);

                                    return clientFactory.createRevisionedStreamClient(logName, new RecordsSerializer<>(),
                                            SynchronizerConfig.builder().build());
                                }
                            });
        
        this.recordCache =
                CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .build(new CacheLoader<RecordCacheKey, RecordCacheValue>() {
                                @ParametersAreNonnullByDefault
                                @Override
                                @SneakyThrows
                                public RecordCacheValue load(final RecordCacheKey key) {
                                    RevisionedStreamClient<Record> cl = clientCache.get(key.clientCacheKey);
                                    
                                    Iterator<Map.Entry<Revision, Record>> records = cl.readFrom(key.revision);
                                    AtomicReference<RecordCacheValue> recordCacheValue = new AtomicReference<>();
                                    Revision current = key.revision;
                                    while (records.hasNext()) {
                                        Map.Entry<Revision, Record> entry = records.next();
                                        Revision next = entry.getKey();
                                        
                                        RecordCacheValue value = new RecordCacheValue(entry.getValue(), next);
                                        recordCacheValue.compareAndSet(null, value);
                                        RecordCacheKey recordCacheKey = new RecordCacheKey(key.clientCacheKey, current);
                                        recordCache.put(recordCacheKey, value);
                                        current = next;
                                    }
                                    if (recordCacheValue.get() == null) {
                                        String error = String.format("group=%s,position=%s", key.clientCacheKey.getGroup(), key.getRevision().toString());
                                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, error);
                                    }
                                    return recordCacheValue.get();
                                }
                            });
    }

    @SneakyThrows(ExecutionException.class)
    public RecordCacheValue getRecord(RecordCacheKey recordCacheKey) {
        try {
            return recordCache.get(recordCacheKey);
        } catch (UncheckedExecutionException ex) {
            throw new CompletionException(ex.getCause());
        }
    }
    
    public void putRecord(RecordCacheKey key, RecordCacheValue record) {
        recordCache.put(key, record);    
    }
    
    @SneakyThrows(ExecutionException.class)
    public RevisionedStreamClient<Record> getClient(ClientCacheKey clientCacheKey) {
        return clientCache.get(clientCacheKey);
    }
    
    @Data
    static class RecordCacheKey {
        private final ClientCacheKey clientCacheKey;
        private final Revision revision;
    }
    
    @Data
    static class RecordCacheValue {
        private final Record record;
        private final Revision nextRevision;
    }
    
    @Data
    static class ClientCacheKey {
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
