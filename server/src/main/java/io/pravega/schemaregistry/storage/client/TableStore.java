/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.buffer.ByteBuf;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.util.RetryHelper;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionDropped;
import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionFailed;

/**
 * Wrapper class over {@link PravegaTablesStoreHelper}. Its implementation abstracts the caller classes from the library
 * used for interacting with pravega tables.
 */
@Slf4j
public class TableStore {
    public static final String SCHEMA_REGISTRY_SCOPE = "_schemaregistry";
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final HostStoreImpl hostStore;
    private final int numOfRetries;
    private final ScheduledExecutorService executor;
    private final Cache<TableCacheKey, Version.VersionedRecord<?>> cache;
    private final Function<String, String> tokenSupplier;
    private final CompletableFuture<Void> createScope;
    private final Cache<String, String> tokenCache;
    
    public TableStore(ClientConfig clientConfig, ServiceConfig serviceConfig, ScheduledExecutorService executor) {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
        hostStore = new HostStoreImpl(clientConfig, executor);
        segmentHelper = new SegmentHelper(connectionFactory, hostStore);
        this.executor = executor;
        this.tokenSupplier = x -> serviceConfig.isAuthEnabled() ? 
                hostStore.getController().getOrRefreshDelegationTokenFor(SCHEMA_REGISTRY_SCOPE, x).join() : "";
        numOfRetries = NUM_OF_RETRIES;
        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(10000)
                                 .build();

        tokenCache = CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .build();
        this.createScope = createScope(); 
    }
    
    public CompletableFuture<Void> createScope() {
        return Retry.indefinitelyWithExpBackoff("Failed to create scope. Retrying...")
                    .runAsync(() -> Futures.toVoid(hostStore.getController().createScope(SCHEMA_REGISTRY_SCOPE)), executor);
    }
    
    public CompletableFuture<Void> createTable(String tableName) {
        log.debug("create table called for table: {}", tableName);

        return Futures.toVoid(withRetries(() -> createScope.thenCompose(v -> segmentHelper.createTableSegment(
                tableName, getToken(tableName), RequestTag.NON_EXISTENT_ID)),
                () -> String.format("create table: %s", tableName), tableName))
                      .whenCompleteAsync((r, e) -> {
                          if (e != null) {
                              log.warn("create table {} threw exception", tableName, e);
                          } else {
                              log.debug("table {} created successfully", tableName);
                          }
                      }, executor);
    }

    public CompletableFuture<Void> deleteTable(String tableName, boolean mustBeEmpty) {
        log.debug("delete table called for table: {}", tableName);
        return withRetries(() -> segmentHelper.deleteTableSegment(
                tableName, mustBeEmpty, getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("delete table: %s", tableName), tableName)
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                })
                .thenAcceptAsync(v -> log.debug("table {} deleted successfully", tableName), executor);
    }

    public CompletableFuture<Void> addNewEntryIfAbsent(String tableName, byte[] key, @NonNull byte[] value) {
        Map<byte[], Map.Entry<byte[], Version>> batch = new HashMap<>();
        batch.put(key, new AbstractMap.SimpleEntry<>(value, null));
        return Futures.toVoid(updateEntries(tableName, batch)
                .thenApply(list -> list.get(0)))
                      .exceptionally(e -> {
                          if (Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException) {
                              return null;
                          } else {
                              throw new CompletionException(e);
                          }
                      });
    }

    public CompletableFuture<Version> updateEntry(String tableName, byte[] key, byte[] value, Version ver) {
        Map<byte[], Map.Entry<byte[], Version>> batch = new HashMap<>();
        batch.put(key, new AbstractMap.SimpleEntry<>(value, ver));
        return updateEntries(tableName, batch).thenApply(list -> list.get(0));
    }

    public CompletableFuture<List<Version>> updateEntries(String tableName, Map<byte[], Map.Entry<byte[], Version>> batch) {
        return withRetries(() -> {
            List<TableEntry<byte[], byte[]>> entries = batch.entrySet().stream().map(x -> {
                KeyVersion version = x.getValue().getValue() == null ? KeyVersion.NOT_EXISTS :
                        new KeyVersionImpl(x.getValue().getValue().getVersion());

                return new TableEntryImpl<>(new TableKeyImpl<>(x.getKey(), version), x.getValue().getKey());
            }).collect(Collectors.toList());

            return segmentHelper.updateTableEntries(tableName, entries, getToken(tableName), RequestTag.NON_EXISTENT_ID)
                                .thenApply(list -> list.stream().map(x -> new Version(x.getSegmentVersion()))
                                                       .collect(Collectors.toList()));
        }, () -> String.format("update entries : %s", tableName), tableName, true);
    }

    public <T> CompletableFuture<Version.VersionedRecord<T>> getEntry(String tableName, byte[] key, Function<byte[], T> fromBytes) {
        return getEntries(tableName, Collections.singletonList(key), true)
                .thenApply(records -> {
                    Version.VersionedRecord<byte[]> value = records.get(0);
                    return new Version.VersionedRecord<>(fromBytes.apply(value.getRecord()), value.getVersion());
                });
    }
    
    public CompletableFuture<List<Version.VersionedRecord<byte[]>>> getEntries(String tableName, List<byte[]> tableKeys, boolean throwOnNotFound) {
        log.info("get entries called for : {} key : {}", tableName, tableKeys);
        List<TableKey<byte[]>> keys = tableKeys.stream().map(key -> new TableKeyImpl<>(key, null)).collect(Collectors.toList());

        CompletableFuture<List<Version.VersionedRecord<byte[]>>> result = new CompletableFuture<>();
        String message = "get entries for table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, tableName), tableName)
                .thenApplyAsync(x -> {
                    return x.stream().map(y -> {
                        KeyVersion version = y.getKey().getVersion();
                        if (version.equals(KeyVersion.NOT_EXISTS)) {
                            if (throwOnNotFound) {
                                throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "key not found");
                            } else {
                                return new Version.VersionedRecord<>((byte[]) null, Version.NON_EXISTENT);
                            }
                        } else {
                            return new Version.VersionedRecord<>(y.getValue(),
                                    new Version(version.getSegmentVersion()));
                        }
                    }).collect(Collectors.toList());
                }, executor)
                .whenCompleteAsync((r, e) -> {
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                }, executor);
        return result;
    }

    public CompletableFuture<Void> removeEntry(String tableName, byte[] key) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key, null));
        return withRetries(() -> segmentHelper.removeTableKeys(
                tableName, keys, getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entry: key: %s table: %s", key, tableName), tableName)
                .thenAcceptAsync(v -> log.trace("entry for key {} removed from table {}", key, tableName), executor)
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                });
    }

    public <K> CompletableFuture<List<K>> getAllKeys(String tableName, Function<byte[], K> fromBytesKey) {
        List<K> keys = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, K> iterator = new ContinuationTokenAsyncIterator<>(
                token -> getKeysPaginated(tableName, token, 1000, fromBytesKey)
                        .thenApplyAsync(result -> {
                            token.release();
                            return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                        }, executor),
                IteratorState.EMPTY.toBytes());

        return iterator.collectRemaining(keys::add)
                       .thenApply(v -> keys);
    }

    public <K, T> CompletableFuture<List<Map.Entry<K, Version.VersionedRecord<T>>>> getAllEntries(String tableName,
                                                                                                  Function<byte[], K> fromBytesKey,
                                                                                                  Function<byte[], T> fromBytesValue) {
        List<Map.Entry<K, Version.VersionedRecord<T>>> entries = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, Map.Entry<K, Version.VersionedRecord<T>>> iterator = new ContinuationTokenAsyncIterator<>(
                token -> getEntriesPaginated(tableName, token, 1000, fromBytesKey, fromBytesValue)
                        .thenApplyAsync(result -> {
                            token.release();
                            return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                        }, executor),
                IteratorState.EMPTY.toBytes());

        return iterator.collectRemaining(entries::add).thenApply(v -> entries);
    }

    /**
     * Api to read cached value for the specified key from the requested table.
     *
     * @param table  name of table.
     * @param key    key to query.
     * @param tClass class of object type to deserialize into.
     * @param <K>    Type of key.
     * @param <T>    Type of object to deserialize the response into.
     * @return Returns a completableFuture which when completed will have the deserialized value with its store key version.
     */
    @SuppressWarnings("unchecked")
    public <K, T> Version.VersionedRecord<T> getCachedRecord(String table, K key, Class<T> tClass) {
        return (Version.VersionedRecord<T>) cache.getIfPresent(new TableCacheKey<>(table, key));
    }

    public <K, T> void cacheRecord(String table, K key, Version.VersionedRecord<T> value) {
        cache.put(new TableCacheKey<>(table, key), value);
    }

    public <K, T> void invalidateCache(String table, K key) {
        cache.invalidate(new TableCacheKey<>(table, key));
    }

    public <K> CompletableFuture<Map.Entry<ByteBuf, List<K>>> getKeysPaginated(String tableName, ByteBuf continuationToken, int limit,
                                                                                Function<byte[], K> fromByteKey) {
        log.trace("get keys paginated called for : {}", tableName);

        return withRetries(() ->
                        segmentHelper.readTableKeys(tableName, limit, IteratorState.fromBytes(continuationToken), getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get keys paginated for table: %s", tableName), tableName)
                .thenApplyAsync(result -> {
                    List<K> items = result.getItems().stream().map(x -> fromByteKey.apply(x.getKey()))
                                          .collect(Collectors.toList());
                    log.trace("get keys paginated on table {} returned items {}", tableName, items);
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    private <K, T> CompletableFuture<Map.Entry<ByteBuf, List<Map.Entry<K, Version.VersionedRecord<T>>>>> getEntriesPaginated(
            String tableName, ByteBuf continuationToken, int limit, Function<byte[], K> fromBytesKey,
            Function<byte[], T> fromBytesValue) {
        log.trace("get entries paginated called for : {}", tableName);
        return withRetries(() -> segmentHelper.readTableEntries(tableName, limit,
                IteratorState.fromBytes(continuationToken), getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s", tableName), tableName)
                .thenApplyAsync(result -> {
                    List<Map.Entry<K, Version.VersionedRecord<T>>> items = result.getItems().stream().map(x -> {
                        T deserialized = fromBytesValue.apply(x.getValue());
                        Version.VersionedRecord<T> value = new Version.VersionedRecord<>(deserialized, new Version(x.getKey().getVersion().getSegmentVersion()));
                        return new AbstractMap.SimpleEntry<>(fromBytesKey.apply(x.getKey().getKey()), value);
                    }).collect(Collectors.toList());
                    log.trace("get keys paginated on table {} returned number of items {}", tableName, items.size());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future, Supplier<String> errorMessageSupplier,
                                                                   String tableName, boolean throwOriginalOnCfe) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                        toThrow = throwOriginalOnCfe ? wcfe :
                                StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        hostStore.invalidateCache(tableName);
                        break;
                    case UnknownHost:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        hostStore.invalidateCache(tableName);
                        break;
                    case AuthFailed:
                        tokenCache.invalidate(tableName);
                        toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyDoesNotExist:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, wcfe, errorMessage);
                        break;
                    case TableKeyBadVersion:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, wcfe, errorMessage);
                        break;
                    default:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.UNKNOWN, wcfe, errorMessage);
                }
            } else if (cause instanceof HostStoreException) {
                log.warn("Host Store exception {}", cause.getMessage());
                toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, cause, errorMessage);
            } else {
                log.warn("exception of unknown type thrown {} ", errorMessage, cause);
                toThrow = StoreExceptions.create(StoreExceptions.Type.UNKNOWN, cause, errorMessage);
            }

            throw new CompletionException(toThrow);
        });
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage, String tableName) {
        return withRetries(futureSupplier, errorMessage, tableName, false);
    }

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage,
                                                 String tableName, boolean throwOriginalOnCfe) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage, tableName, throwOriginalOnCfe),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.StoreConnectionException, numOfRetries, executor)
                          .exceptionally(e -> {
                              Throwable t = Exceptions.unwrap(e);
                              if (t instanceof RetriesExhaustedException) {
                                  throw new CompletionException(t.getCause());
                              } else {
                                  Throwable unwrap = Exceptions.unwrap(e);
                                  if (unwrap instanceof WireCommandFailedException &&
                                          (((WireCommandFailedException) unwrap).getReason().equals(ConnectionDropped) ||
                                                  ((WireCommandFailedException) unwrap).getReason().equals(ConnectionFailed))) {
                                      throw new CompletionException(StoreException.create(StoreException.Type.CONNECTION_ERROR,
                                              errorMessage.get()));
                                  } else {
                                      throw new CompletionException(unwrap);
                                  }
                              }
                          });
    }
    
    @SneakyThrows
    private String getToken(String tableName) {
        return tokenCache.get(tableName, () ->  tokenSupplier.apply(tableName));
    }

    @Data
    private static class TableCacheKey<K> {
        private final String table;
        private final K key;
    }
}
