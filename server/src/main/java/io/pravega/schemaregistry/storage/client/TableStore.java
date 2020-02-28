/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import io.netty.buffer.ByteBuf;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class TableStore {
    private final PravegaTablesStoreHelper tableStoreHelper;
    private final HostStoreImpl hostStore;
    public TableStore(ClientConfig clientConfig, GrpcAuthHelper authHelper, ScheduledExecutorService executor) {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
        hostStore = new HostStoreImpl(clientConfig, executor);
        SegmentHelper segmentHelper = new SegmentHelper(connectionFactory, hostStore);
        tableStoreHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }
    
    public <T> CompletableFuture<VersionedMetadata<T>> getCachedData(String tableName, String key, Function<byte[], T> fromBytes) {
        return exceptionally(tableName, tableStoreHelper.getCachedData(tableName, key, fromBytes));
    }
    
    public void invalidateCache(String table, String key) {
        tableStoreHelper.invalidateCache(table, key);
    }
    
    public CompletableFuture<Void> createTable(String tableName) {
        return exceptionally(tableName, tableStoreHelper.createTable(tableName));
    }
    
    public CompletableFuture<Void> deleteTable(String tableName, boolean mustBeEmpty) {
        return exceptionally(tableName, tableStoreHelper.deleteTable(tableName, mustBeEmpty));
    }
    
    public CompletableFuture<Version> addNewEntry(String tableName, String key, @NonNull byte[] value) {
        return exceptionally(tableName, tableStoreHelper.addNewEntry(tableName, key, value));
    }
    
    public CompletableFuture<Version> addNewEntryIfAbsent(String tableName, String key, @NonNull byte[] value) {
        return exceptionally(tableName, tableStoreHelper.addNewEntryIfAbsent(tableName, key, value));
    }
    
    public CompletableFuture<Void> addNewEntriesIfAbsent(String tableName, Map<String, byte[]> toAdd) {
        return exceptionally(tableName, tableStoreHelper.addNewEntriesIfAbsent(tableName, toAdd));
    }
    
    public CompletableFuture<Version> updateEntry(String tableName, String key, byte[] value, Version ver) {
        return exceptionally(tableName, tableStoreHelper.updateEntry(tableName, key, value, ver));
    }
    
    public <T> CompletableFuture<VersionedMetadata<T>> getEntry(String tableName, String key, Function<byte[], T> fromBytes) {
        return exceptionally(tableName, tableStoreHelper.getEntry(tableName, key, fromBytes));
    }

    public CompletableFuture<Void> removeEntry(String tableName, String key) {
        return exceptionally(tableName, tableStoreHelper.removeEntry(tableName, key));
    }

    public CompletableFuture<Void> removeEntry(String tableName, String key, Version ver) {
        return exceptionally(tableName, tableStoreHelper.removeEntry(tableName, key, ver));
    }

    public CompletableFuture<Void> removeEntries(String tableName, Collection<String> keys) {
        return exceptionally(tableName, tableStoreHelper.removeEntries(tableName, keys));
    }

    public CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String tableName, ByteBuf continuationToken, int limit) {
        return exceptionally(tableName, tableStoreHelper.getKeysPaginated(tableName, continuationToken, limit));
    }

    public <T> CompletableFuture<Map.Entry<ByteBuf, List<Map.Entry<String, VersionedMetadata<T>>>>> getEntriesPaginated(String tableName, ByteBuf continuationToken, int limit, Function<byte[], T> fromBytes) {
        return exceptionally(tableName, tableStoreHelper.getEntriesPaginated(tableName, continuationToken, limit, fromBytes));
    }

    public <K, V> CompletableFuture<Map<K, V>> getEntriesWithFilter(String tableName, Function<String, K> fromStringKey, Function<byte[], V> fromBytesValue, BiFunction<K, V, Boolean> filter, int limit) {
        return exceptionally(tableName, tableStoreHelper.getEntriesWithFilter(tableName, fromStringKey, fromBytesValue, filter, limit));
    }

    public CompletableFuture<List<String>> getAllKeys(String tableName) {
        List<String> keys = new LinkedList<>();
        return tableStoreHelper.getAllKeys(tableName)
                .collectRemaining(keys::add)
                .thenApply(v -> keys);
    }

    public <T> AsyncIterator<Map.Entry<String, VersionedMetadata<T>>> getAllEntries(String tableName, Function<byte[], T> fromBytes) {
        return tableStoreHelper.getAllEntries(tableName, fromBytes);
    }
    
    private <T> CompletableFuture<T> exceptionally(String tableName, CompletableFuture<T> future) {
        return future.exceptionally(e -> {
            Throwable unwrap = Exceptions.unwrap(e);
            if (unwrap instanceof StoreException.StoreConnectionException) {
                hostStore.invalidateCache(tableName);
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, unwrap));
            } else if (unwrap instanceof StoreException.DataNotFoundException) {
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, unwrap));
            } else if (unwrap instanceof StoreException.DataExistsException) {
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.DATA_EXISTS, unwrap));
            } else if (unwrap instanceof StoreException.DataNotEmptyException) {
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.DATA_NOT_EMPTY, unwrap));
            } else if (unwrap instanceof StoreException.WriteConflictException) {
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, unwrap));
            } else {
                throw new CompletionException(StoreExceptions.create(StoreExceptions.Type.UNKNOWN, unwrap));
            }
        });
    }
}
