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
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostStoreException;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.util.RetryHelper;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Wrapper class over {@link PravegaTablesStoreHelper}. Its implementation abstracts the caller classes from the library
 * used for interacting with pravega tables. 
 */
@Slf4j
public class TableStore {
    private static final int NUM_OF_RETRIES = 15; // approximately 1 minute worth of retries
    private final SegmentHelper segmentHelper;
    private final HostStoreImpl hostStore;
    private final GrpcAuthHelper authHelper;
    private final int numOfRetries;
    private final ScheduledExecutorService executor;
    private final AtomicReference<String> authToken;

    public TableStore(ClientConfig clientConfig, GrpcAuthHelper authHelper, ScheduledExecutorService executor) {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
        this.authHelper = authHelper;
        hostStore = new HostStoreImpl(clientConfig, executor);
        segmentHelper = new SegmentHelper(connectionFactory, hostStore);
        this.executor = executor;
        this.authToken = new AtomicReference<>(authHelper.retrieveMasterToken());
        numOfRetries = NUM_OF_RETRIES;
    }

    public CompletableFuture<Void> createTable(String tableName) {
        log.debug("create table called for table: {}", tableName);

        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(tableName, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("create table: %s", tableName)))
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
                tableName, mustBeEmpty, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("delete table: %s", tableName))
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                })
                .thenAcceptAsync(v -> log.debug("table {} deleted successfully", tableName), executor);
    }

    public CompletableFuture<Void> addNewEntryIfAbsent(String tableName, String key, @NonNull byte[] value) {
        Map<String, Map.Entry<byte[], Version>> batch = new HashMap<>();
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

    public CompletableFuture<Version> updateEntry(String tableName, String key, byte[] value, Version ver) {
        Map<String, Map.Entry<byte[], Version>> batch = new HashMap<>();
        batch.put(key, new AbstractMap.SimpleEntry<>(value, ver));
        return updateEntries(tableName, batch).thenApply(list -> list.get(0));
    }

    public CompletableFuture<List<Version>> updateEntries(String tableName, Map<String, Map.Entry<byte[], Version>> batch) {
        return withRetries(() -> {
            List<TableEntry<byte[], byte[]>> entries = batch.entrySet().stream().map(x -> {
                KeyVersion version = x.getValue().getValue() == null ? KeyVersion.NOT_EXISTS : 
                        new KeyVersionImpl(x.getValue().getValue().asLongVersion().getLongValue());

                return new TableEntryImpl<>(new TableKeyImpl<>(x.getKey().getBytes(Charsets.UTF_8), version), x.getValue().getKey());
            }).collect(Collectors.toList());

            return segmentHelper.updateTableEntries(tableName, entries, authHelper.retrieveMasterToken(), RequestTag.NON_EXISTENT_ID)
                                .thenApply(list -> list.stream().map(x -> Version.LongVersion.builder().longValue(x.getSegmentVersion()).build())
                                                       .collect(Collectors.toList()));
        }, () -> String.format("update entries : %s", tableName));
    }

    public <T> CompletableFuture<VersionedMetadata<T>> getEntry(String tableName, String key, Function<byte[], T> fromBytes) {
        return getEntries(tableName, Collections.singletonList(key))
                .thenApply(records -> {
                    VersionedMetadata<byte[]> value = records.get(0);
                    return new VersionedMetadata<>(fromBytes.apply(value.getObject()), value.getVersion());
                });
    }

    public CompletableFuture<List<VersionedMetadata<byte[]>>> getEntries(String tableName, List<String> tableKeys) {
        log.info("get entries called for : {} key : {}", tableName, tableKeys);
        List<TableKey<byte[]>> keys = tableKeys.stream().map(key -> new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null)).collect(Collectors.toList());

        CompletableFuture<List<VersionedMetadata<byte[]>>> result = new CompletableFuture<>();
        String message = "get entries for table: %s";
        withRetries(() -> segmentHelper.readTable(tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format(message, tableName))
                .thenApplyAsync(x -> {
                    return x.stream().map(y -> {
                        KeyVersion version = y.getKey().getVersion();
                        if (version.equals(KeyVersion.NOT_EXISTS)) {
                            throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "key not found");   
                        } else {
                            return new VersionedMetadata<>(y.getValue(),
                                    Version.LongVersion.builder().longValue(version.getSegmentVersion()).build());
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

    public CompletableFuture<Void> removeEntry(String tableName, String key) {
        log.trace("remove entry called for : {} key : {}", tableName, key);
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(key.getBytes(Charsets.UTF_8), null));
        return withRetries(() -> segmentHelper.removeTableKeys(
                tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("remove entry: key: %s table: %s", key, tableName))
                .thenAcceptAsync(v -> log.trace("entry for key {} removed from table {}", key, tableName), executor)
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                });
    }

    public CompletableFuture<List<String>> getAllKeys(String tableName) {
        List<String> keys = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, String> iterator = new ContinuationTokenAsyncIterator<>(token -> getKeysPaginated(tableName, token, 1000)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorState.EMPTY.toBytes());

        return iterator.collectRemaining(keys::add)
                       .thenApply(v -> keys);
    }

    public <T> CompletableFuture<List<Map.Entry<String, VersionedMetadata<T>>>> getAllEntries(String tableName, Function<byte[], T> fromBytes) {
        List<Map.Entry<String, VersionedMetadata<T>>> entries = new LinkedList<>();
        ContinuationTokenAsyncIterator<ByteBuf, Map.Entry<String, VersionedMetadata<T>>> iterator = new ContinuationTokenAsyncIterator<>(token -> getEntriesPaginated(tableName, token, 1000, fromBytes)
                .thenApplyAsync(result -> {
                    token.release();
                    return new AbstractMap.SimpleEntry<>(result.getKey(), result.getValue());
                }, executor),
                IteratorState.EMPTY.toBytes());

        return iterator.collectRemaining(entries::add)
                       .thenApply(v -> entries);
    }

    private CompletableFuture<Map.Entry<ByteBuf, List<String>>> getKeysPaginated(String tableName, ByteBuf continuationToken, int limit) {
        log.trace("get keys paginated called for : {}", tableName);

        return withRetries(() ->
                        segmentHelper.readTableKeys(tableName, limit, IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get keys paginated for table: %s", tableName))
                .thenApplyAsync(result -> {
                    List<String> items = result.getItems().stream().map(x -> new String(x.getKey(), Charsets.UTF_8))
                                               .collect(Collectors.toList());
                    log.trace("get keys paginated on table {} returned items {}", tableName, items);
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    private <T> CompletableFuture<Map.Entry<ByteBuf, List<Map.Entry<String, VersionedMetadata<T>>>>> getEntriesPaginated(
            String tableName, ByteBuf continuationToken, int limit,
            Function<byte[], T> fromBytes) {
        log.trace("get entries paginated called for : {}", tableName);
        return withRetries(() -> segmentHelper.readTableEntries(tableName, limit,
                IteratorState.fromBytes(continuationToken), authToken.get(), RequestTag.NON_EXISTENT_ID),
                () -> String.format("get entries paginated for table: %s", tableName))
                .thenApplyAsync(result -> {
                    List<Map.Entry<String, VersionedMetadata<T>>> items = result.getItems().stream().map(x -> {
                        String key = new String(x.getKey().getKey(), Charsets.UTF_8);
                        T deserialized = fromBytes.apply(x.getValue());
                        VersionedMetadata<T> value = new VersionedMetadata<>(deserialized, Version.LongVersion.builder().longValue(x.getKey().getVersion().getSegmentVersion()).build());
                        return new AbstractMap.SimpleEntry<>(key, value);
                    }).collect(Collectors.toList());
                    log.trace("get keys paginated on table {} returned number of items {}", tableName, items.size());
                    return new AbstractMap.SimpleEntry<>(result.getState().toBytes(), items);
                }, executor);
    }

    private <T> Supplier<CompletableFuture<T>> exceptionalCallback(Supplier<CompletableFuture<T>> future, Supplier<String> errorMessageSupplier) {
        return () -> CompletableFuture.completedFuture(null).thenComposeAsync(v -> future.get(), executor).exceptionally(t -> {
            String errorMessage = errorMessageSupplier.get();
            Throwable cause = Exceptions.unwrap(t);
            Throwable toThrow;
            if (cause instanceof WireCommandFailedException) {
                WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                switch (wcfe.getReason()) {
                    case ConnectionDropped:
                    case ConnectionFailed:
                    case UnknownHost:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case AuthFailed:
                        authToken.set(authHelper.retrieveMasterToken());
                        toThrow = StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, wcfe, errorMessage);
                        break;
                    case SegmentDoesNotExist:
                        toThrow = StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, wcfe, errorMessage);
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

    private <T> CompletableFuture<T> withRetries(Supplier<CompletableFuture<T>> futureSupplier, Supplier<String> errorMessage) {
        return RetryHelper.withRetriesAsync(exceptionalCallback(futureSupplier, errorMessage),
                e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    return unwrap instanceof StoreExceptions.StoreConnectionException;
                }, numOfRetries, executor)
                          .exceptionally(e -> {
                              Throwable t = Exceptions.unwrap(e);
                              if (t instanceof RetriesExhaustedException) {
                                  throw new CompletionException(t.getCause());
                              } else {
                                  throw new CompletionException(t);
                              }
                          });
    }

}
