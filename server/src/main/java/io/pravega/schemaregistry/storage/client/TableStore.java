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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.AbstractService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.util.RetryHelper;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionDropped;
import static io.pravega.controller.server.WireCommandFailedException.Reason.ConnectionFailed;

/**
 * Wrapper class over {@link SegmentHelper}. Its implementation abstracts the caller classes from the library
 * used for interacting with pravega tables.
 */
@Slf4j
public class TableStore extends AbstractService {
    public static final String SCHEMA_REGISTRY_SCOPE = "_schemaregistry";
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    /**
     * Segment helper to make KVT wire command calls to segment store. 
     */
    private final SegmentHelper segmentHelper;
    /**
     * Host Store implementation required by {@link SegmentHelper}. This wraps a controller client to get the URI for
     * a segment store instance. 
     */
    private final HostStoreImpl hostStore;
    private final int numOfRetries;
    private final ScheduledExecutorService executor;
    /**
     * Cache where callers can cache values against a table name and a key. 
     */
    private final Cache<TableCacheKey, VersionedRecord<?>> cache;
    /**
     * Function to get delegation token to talk to segment store.
     */
    private final Function<String, String> tokenSupplier;
    /**
     * cache to store the delegation token by segment store instance so that we continue to reuse the token until it 
     * expires, which is when it should be invalidated from the cache explicitly. 
     */
    private final Cache<String, String> tokenCache;

    public TableStore(ClientConfig clientConfig, ScheduledExecutorService executor) {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
        hostStore = new HostStoreImpl(clientConfig, executor);
        segmentHelper = new SegmentHelper(connectionFactory, hostStore);
        this.executor = executor;
        this.tokenSupplier = x -> {
            String[] splits = x.split("/");
            return hostStore.getController().getOrRefreshDelegationTokenFor(splits[0], splits[1]).join();
        };
        numOfRetries = NUM_OF_RETRIES;
        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(10000)
                                 .build();

        tokenCache = CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .build(); 
    }

    @Override
    protected void doStart() {
        createScope()
                .whenComplete((v, e) -> {
                    if (e == null) {
                        notifyStarted();
                    } else {
                        notifyFailed(e);
                    }
                });
    }

    @Override
    protected void doStop() {
        
    }

    public CompletableFuture<Void> createScope() {
        return withRetries(() -> Futures.toVoid(hostStore.getController().createScope(SCHEMA_REGISTRY_SCOPE)),
                () -> "Failed to create scope. Retrying...", "");
    }
    
    public CompletableFuture<Void> createTable(String tableName) {
        log.debug("create table called for table: {}", tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(
                tableName, getToken(tableName), RequestTag.NON_EXISTENT_ID, false),
                () -> String.format("create table: %s", tableName), tableName))
                      .whenComplete((r, e) -> {
                          if (e != null) {
                              log.warn("create table {} threw exception", tableName, e);
                          } else {
                              log.debug("table {} created successfully", tableName);
                          }
                      });
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
                .thenAccept(v -> log.debug("table {} deleted successfully", tableName));
    }

    public CompletableFuture<Void> addNewEntryIfAbsent(String tableName, byte[] key, @NonNull byte[] value) {
        return Futures.toVoid(updateEntries(tableName, Collections.singletonMap(key, new VersionedRecord<>(value, null)))
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
        return updateEntries(tableName, Collections.singletonMap(key, new VersionedRecord<>(value, ver))).thenApply(list -> list.get(0));
    }

    public CompletableFuture<List<Version>> updateEntries(String tableName, Map<byte[], VersionedRecord<byte[]>> batch) {
        Preconditions.checkNotNull(batch);
        List<TableSegmentEntry> entries = batch.entrySet().stream().map(x -> {
            return x.getValue().getVersion() == null ?
                    TableSegmentEntry.notExists(x.getKey(), x.getValue().getRecord()) :
                    TableSegmentEntry.versioned(x.getKey(), x.getValue().getRecord(), x.getValue().getVersion().toLong());
        }).collect(Collectors.toList());
        return withRetries(() -> {
            return segmentHelper.updateTableEntries(tableName, entries, getToken(tableName), RequestTag.NON_EXISTENT_ID)
                                .thenApply(list -> list.stream().map(x -> new Version(x.getSegmentVersion()))
                                                       .collect(Collectors.toList()));
        }, () -> String.format("update entries : %s", tableName), tableName, true)
                .whenComplete((r, e) -> {
                    releaseEntries(entries);
                });
    }

    public <T> CompletableFuture<VersionedRecord<T>> getEntry(String tableName, byte[] key, Function<byte[], T> fromBytes) {
        return getEntries(tableName, Collections.singletonList(key), true)
                .thenApply(records -> {
                    VersionedRecord<byte[]> value = records.get(0);
                    return new VersionedRecord<>(fromBytes.apply(value.getRecord()), value.getVersion());
                });
    }

    public CompletableFuture<List<VersionedRecord<byte[]>>> getEntries(String tableName, List<byte[]> tableKeys, boolean throwOnNotFound) {
        log.info("get entries called for : {} key : {}", tableName, tableKeys);
        List<TableSegmentKey> keys = tableKeys.stream().map(TableSegmentKey::unversioned).collect(Collectors.toList());

        CompletableFuture<List<VersionedRecord<byte[]>>> result = new CompletableFuture<>();
        String message = "get entries for table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, tableName), tableName)
                .thenApply(entriesFromStore -> {
                    try {
                        List<VersionedRecord<byte[]>> entries = entriesFromStore.stream().map(y -> {
                            TableSegmentKeyVersion version = y.getKey().getVersion();
                            if (version.equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                                if (throwOnNotFound) {
                                    throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "key not found");
                                } else {
                                    return new VersionedRecord<>((byte[]) null, Version.NON_EXISTENT);
                                }
                            } else {
                                return new VersionedRecord<>(getArray(y.getValue()), new Version(version.getSegmentVersion()));
                            }
                        }).collect(Collectors.toList());
                        return entries;
                    } finally {
                        releaseEntries(entriesFromStore);
                    }
                })
                .whenComplete((r, e) -> {
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        result.complete(r);
                    }
                    releaseKeys(keys);
                });
        return result;
    }

    public CompletableFuture<Void> removeEntry(String tableName, byte[] key) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        List<TableSegmentKey> keys = Collections.singletonList(TableSegmentKey.unversioned(key));
        return withRetries(() -> segmentHelper.removeTableKeys(
                tableName, keys, getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entry: key: %s table: %s", key, tableName), tableName)
                .thenAccept(v -> log.trace("entry for key {} removed from table {}", key, tableName))
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                }).whenComplete((r, e) -> {
                    releaseKeys(keys);
                });
    }

    public <K> CompletableFuture<List<K>> getAllKeys(String tableName, Function<byte[], K> fromBytesKey) {
        List<K> keys = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, K> iterator = new ContinuationTokenAsyncIterator<>(
                token -> getKeysPaginated(tableName, token, 1000, fromBytesKey)
                        .thenApply(result -> {
                            token.release();
                            return new AbstractMap.SimpleEntry<>(result.getToken(), result.getList());
                        }),
                IteratorStateImpl.EMPTY.getToken());

        return iterator.collectRemaining(keys::add)
                       .thenApply(v -> keys);
    }

    public <K, T> CompletableFuture<List<VersionedEntry<K, T>>> getAllEntries(String tableName,
                                                                              Function<byte[], K> fromBytesKey,
                                                                              Function<byte[], T> fromBytesValue) {
        List<VersionedEntry<K, T>> entries = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, VersionedEntry<K, T>> iterator = new ContinuationTokenAsyncIterator<>(
                token -> getEntriesPaginated(tableName, token, 1000, fromBytesKey, fromBytesValue)
                        .thenApply(result -> {
                            token.release();
                            return new AbstractMap.SimpleEntry<>(result.getToken(), result.getList());
                        }),
                IteratorStateImpl.EMPTY.getToken());

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
    public <K, T> VersionedRecord<T> getCachedRecord(String table, K key, Class<T> tClass) {
        return (VersionedRecord<T>) cache.getIfPresent(new TableCacheKey<>(table, key));
    }

    public <K, T> void cacheRecord(String table, K key, VersionedRecord<T> value) {
        cache.put(new TableCacheKey<>(table, key), value);
    }

    public <K, T> void invalidateCache(String table, K key) {
        cache.invalidate(new TableCacheKey<>(table, key));
    }

    public <K> CompletableFuture<ResultPage<K, ByteBuf>> getKeysPaginated(String tableName, ByteBuf continuationToken, int limit,
                                                                          Function<byte[], K> fromByteKey) {
        log.trace("get keys paginated called for : {}", tableName);

        return withRetries(() ->
                        segmentHelper.readTableKeys(tableName, limit, IteratorStateImpl.fromBytes(continuationToken),
                                getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get keys paginated for table: %s", tableName), tableName)
                .thenApply(result -> {
                    try {
                        List<K> items = result.getItems().stream().map(x -> fromByteKey.apply(getArray(x.getKey())))
                                              .collect(Collectors.toList());
                        log.trace("get keys paginated on table {} returned items {}", tableName, items);
                        return new ResultPage<>(items, getNextToken(continuationToken, result));
                    } finally {
                        releaseKeys(result.getItems());
                    }
                });
    }

    private <K, T> CompletableFuture<ResultPage<VersionedEntry<K, T>, ByteBuf>> getEntriesPaginated(
            String tableName, ByteBuf continuationToken, int limit, Function<byte[], K> fromBytesKey,
            Function<byte[], T> fromBytesValue) {
        log.trace("get entries paginated called for : {}", tableName);
        return withRetries(() -> segmentHelper.readTableEntries(tableName, limit,
                IteratorStateImpl.fromBytes(continuationToken), getToken(tableName), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s", tableName), tableName)
                .thenApply(result -> {
                    try {
                        List<VersionedEntry<K, T>> items = result.getItems().stream().map(x -> {
                            T deserialized = fromBytesValue.apply(getArray(x.getValue()));
                            VersionedRecord<T> value = new VersionedRecord<>(deserialized, new Version(x.getKey().getVersion().getSegmentVersion()));
                            return new VersionedEntry<>(fromBytesKey.apply(getArray(x.getKey().getKey())), value);
                        }).collect(Collectors.toList());

                        log.trace("get keys paginated on table {} returned number of items {}", tableName, items.size());
                        return new ResultPage<>(items, getNextToken(continuationToken, result));
                    } finally {
                        releaseEntries(result.getItems());
                    }
                });
    }

    private ByteBuf getNextToken(ByteBuf continuationToken, IteratorItem<?> result) {
        return result.getItems().isEmpty() && result.getState().isEmpty() ?
                continuationToken : Unpooled.wrappedBuffer(result.getState().toBytes());
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
    
    @SneakyThrows(ExecutionException.class)
    private String getToken(String tableName) {
        return tokenCache.get(tableName, () ->  tokenSupplier.apply(tableName));
    }
    
    @Data
    private static class TableCacheKey<K> {
        private final String table;
        private final K key;
    }

    private byte[] getArray(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return bytes;
    }

    private void releaseKeys(Collection<TableSegmentKey> keys) {
        for (TableSegmentKey k : keys) {
            ReferenceCountUtil.safeRelease(k.getKey());
        }
    }

    private void releaseEntries(Collection<TableSegmentEntry> entries) {
        for (TableSegmentEntry e : entries) {
            ReferenceCountUtil.safeRelease(e.getKey().getKey());
            ReferenceCountUtil.safeRelease(e.getValue());
        }
    }

}
