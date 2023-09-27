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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.TagLogger;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.AccessLevel;
import lombok.Getter;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;

/**
 * Used for making wire command calls into Segment Store.  
 */
public class WireCommandClient {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(WireCommandClient.class));

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_SUCCESS_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .put(WireCommands.CreateTableSegment.class, ImmutableSet.of(WireCommands.SegmentCreated.class,
                            WireCommands.SegmentAlreadyExists.class))
                    .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.SegmentDeleted.class,
                            WireCommands.NoSuchSegment.class))
                    .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesUpdated.class))
                    .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRemoved.class,
                            WireCommands.TableKeyDoesNotExist.class))
                    .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.TableRead.class))
                    .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRead.class))
                    .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesRead.class))
                    .build();

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_FAILING_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableKeyDoesNotExist.class,
                            WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
                    .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
                    .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.TableSegmentNotEmpty.class))
                    .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
                    .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
                    .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
                    .build();

    @Getter(AccessLevel.PACKAGE)
    private final HostStore hostStore;
    private final ConnectionPool connectionPool;

    WireCommandClient(final ConnectionPool connectionPool, HostStore hostStore) {
        this.connectionPool = connectionPool;
        this.hostStore = hostStore;
    }

    CompletableFuture<NodeUri> getTableUri(final String tableName) {
        return hostStore.getHostForTableSegment(tableName);
    }

    /**
     * This method sends a WireCommand to create a table segment.
     *
     * @param tableName           Qualified table name.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment creation completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link StoreExceptions}.
     */
    CompletableFuture<Void> createTableSegment(final String tableName,
                                               String delegationToken) {
        return getTableUri(tableName)
                .thenCompose(uri -> {
                    final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;

                    RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
                    final long requestId = connection.getFlow().asLong();

                    return sendRequest(connection, requestId, new WireCommands.CreateTableSegment(requestId, tableName, false, 0, delegationToken, 0))
                            .thenAccept(rpl -> handleReply(rpl, connection, tableName, WireCommands.CreateTableSegment.class, type));
                });
    }

    /**
     * This method sends a WireCommand to delete a table segment.
     *
     * @param tableName           Qualified table name.
     * @param mustBeEmpty         Flag to check if the table segment should be empty before deletion.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment deletion completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link StoreExceptions}.
     */
    CompletableFuture<Void> deleteTableSegment(final String tableName,
                                               final boolean mustBeEmpty,
                                               String delegationToken) {
        return getTableUri(tableName)
                .thenCompose(uri -> {
                    final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;

                    RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
                    final long requestId = connection.getFlow().asLong();

                    return sendRequest(connection, requestId, new WireCommands.DeleteTableSegment(requestId, tableName, mustBeEmpty, delegationToken))
                            .thenAccept(rpl -> handleReply(rpl, connection, tableName, WireCommands.DeleteTableSegment.class, type));
                });
    }

    /**
     * This method sends a WireCommand to update table entries.
     *
     * @param tableName       Qualified table name.
     * @param entries         List of {@link TableSegmentEntry} instances to be updated.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that, when completed normally, will contain the current versions of each
     * {@link TableSegmentEntry}.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be retried
     * then the future will be failed with {@link StoreExceptions}.
     */
    CompletableFuture<List<TableSegmentKeyVersion>> updateTableEntries(final String tableName,
                                                                       final List<TableSegmentEntry> entries,
                                                                       String delegationToken) {
        return getTableUri(tableName).thenCompose(uri -> {
            final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
            List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> wireCommandEntries = entries.stream().map(te -> {
                final WireCommands.TableKey key = convertToWireCommand(te.getKey());
                final WireCommands.TableValue value = new WireCommands.TableValue(te.getValue());
                return new AbstractMap.SimpleImmutableEntry<>(key, value);
            }).collect(Collectors.toList());

            RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
            final long requestId = connection.getFlow().asLong();
            WireCommands.UpdateTableEntries request = new WireCommands.UpdateTableEntries(requestId, tableName, delegationToken,
                    new WireCommands.TableEntries(wireCommandEntries), WireCommands.NULL_TABLE_SEGMENT_OFFSET);

            return sendRequest(connection, requestId, request)
                    .thenApply(rpl -> {
                        handleReply(rpl, connection, tableName, WireCommands.UpdateTableEntries.class, type);
                        return ((WireCommands.TableEntriesUpdated) rpl)
                                .getUpdatedVersions().stream()
                                .map(TableSegmentKeyVersion::from).collect(Collectors.toList());
                    });
        });
    }

    /**
     * This method sends a WireCommand to remove table keys.
     *
     * @param tableName       Qualified table name.
     * @param keys            List of {@link TableSegmentKey}s to be removed. Only if all the elements in the list has version
     *                        as {@link TableSegmentKeyVersion#NO_VERSION} then an unconditional update/removal is performed.
     *                        Else an atomic conditional update (removal) is performed.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed with {@link StoreExceptions}.
     */
    CompletableFuture<Void> removeTableKeys(final String tableName,
                                            final List<TableSegmentKey> keys,
                                            String delegationToken) {
        return getTableUri(tableName).thenCompose(uri -> {
            final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
            List<WireCommands.TableKey> keyList = keys.stream().map(this::convertToWireCommand).collect(Collectors.toList());

            RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
            final long requestId = connection.getFlow().asLong();

            WireCommands.RemoveTableKeys request = new WireCommands.RemoveTableKeys(
                    requestId, tableName, delegationToken, keyList, WireCommands.NULL_TABLE_SEGMENT_OFFSET);

            return sendRequest(connection, requestId, request)
                    .thenAccept(rpl -> handleReply(rpl, connection, tableName, WireCommands.RemoveTableKeys.class, type));
        });
    }

    /**
     * This method sends a WireCommand to read table entries.
     *
     * @param tableName       Qualified table name.
     * @param keys            List of {@link TableSegmentKey}s to be read. {@link TableSegmentKey#getVersion()} is
     *                        not used during this operation and the latest version is read.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that, when completed normally, will contain a list of {@link TableSegmentEntry} with
     * a value corresponding to the latest version. The version will be set to {@link TableSegmentKeyVersion#NOT_EXISTS}
     * if the key does not exist. If the operation failed, the future will be failed with the causing exception.
     */
    CompletableFuture<List<TableSegmentEntry>> readTable(final String tableName,
                                                         final List<TableSegmentKey> keys,
                                                         String delegationToken) {
        return getTableUri(tableName).thenCompose(uri -> {
            final WireCommandType type = WireCommandType.READ_TABLE;
            // the version is always NO_VERSION as read returns the latest version of value.
            List<WireCommands.TableKey> keyList = keys
                    .stream().map(k -> new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion()))
                    .collect(Collectors.toList());

            RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
            final long requestId = connection.getFlow().asLong();

            WireCommands.ReadTable request = new WireCommands.ReadTable(requestId, tableName, delegationToken, keyList);
            return sendRequest(connection, requestId, request)
                    .thenApply(rpl -> {
                        handleReply(rpl, connection, tableName, WireCommands.ReadTable.class, type);
                        return ((WireCommands.TableRead) rpl)
                                .getEntries().getEntries().stream()
                                .map(this::convertFromWireCommand)
                                .collect(Collectors.toList());
                    });
        });
    }

    /**
     * The method sends a WireCommand to iterate over table keys.
     *
     * @param tableName         Qualified table name.
     * @param suggestedKeyCount Suggested number of {@link TableSegmentKey}s to be returned by the SegmentStore.
     * @param state             Last known state of the iterator.
     * @param delegationToken   The token to be presented to the Segment Store.
     * @return A CompletableFuture that will return the next set of {@link TableSegmentKey}s returned from the SegmentStore.
     */
    CompletableFuture<HashTableIteratorItem<TableSegmentKey>> readTableKeys(final String tableName,
                                                                   final int suggestedKeyCount,
                                                                   final HashTableIteratorItem.State state,
                                                                   final String delegationToken) {

        return getTableUri(tableName).thenCompose(uri -> {
            final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
            RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
            final long requestId = connection.getFlow().asLong();

            final HashTableIteratorItem.State token = (state == null) ? HashTableIteratorItem.State.EMPTY : state;

            WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(token.getToken(), Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
            WireCommands.ReadTableKeys request = new WireCommands.ReadTableKeys(requestId, tableName, delegationToken, suggestedKeyCount,
                    args);
            return sendRequest(connection, requestId, request)
                    .thenApply(rpl -> {
                        handleReply(rpl, connection, tableName, WireCommands.ReadTableKeys.class, type);
                        WireCommands.TableKeysRead tableKeysRead = (WireCommands.TableKeysRead) rpl;
                        final HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(tableKeysRead.getContinuationToken());
                        final List<TableSegmentKey> keys =
                                tableKeysRead.getKeys().stream().map(k -> TableSegmentKey.versioned(k.getData(),
                                        k.getKeyVersion())).collect(Collectors.toList());
                        return new HashTableIteratorItem<>(newState, keys);
                    });
        });
    }

    /**
     * The method sends a WireCommand to iterate over table entries.
     *
     * @param tableName           Qualified table name.
     * @param suggestedEntryCount Suggested number of {@link TableSegmentEntry} instances to be returned by the Segment Store.
     * @param state               Last known state of the iterator.
     * @param delegationToken     The token to be presented to the Segment Store.
     * @return A CompletableFuture that will return the next set of {@link TableSegmentEntry} instances returned from the
     * SegmentStore.
     */
    CompletableFuture<HashTableIteratorItem<TableSegmentEntry>> readTableEntries(final String tableName,
                                                                        final int suggestedEntryCount,
                                                                        final HashTableIteratorItem.State state,
                                                                        final String delegationToken) {

        return getTableUri(tableName).thenCompose(uri -> {
            final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
            RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
            final long requestId = connection.getFlow().asLong();

            final HashTableIteratorItem.State token = (state == null) ? HashTableIteratorItem.State.EMPTY : state;

            WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(token.getToken(), Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
            WireCommands.ReadTableEntries request = new WireCommands.ReadTableEntries(requestId, tableName, delegationToken,
                    suggestedEntryCount, args);
            return sendRequest(connection, requestId, request)
                    .thenApply(rpl -> {
                        handleReply(rpl, connection, tableName, WireCommands.ReadTableEntries.class, type);
                        WireCommands.TableEntriesRead tableEntriesRead = (WireCommands.TableEntriesRead) rpl;
                        final HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(tableEntriesRead.getContinuationToken());
                        final List<TableSegmentEntry> entries =
                                tableEntriesRead.getEntries().getEntries().stream()
                                                .map(e -> {
                                                    WireCommands.TableKey k = e.getKey();
                                                    return TableSegmentEntry.versioned(k.getData(),
                                                            e.getValue().getData(),
                                                            k.getKeyVersion());
                                                }).collect(Collectors.toList());
                        return new HashTableIteratorItem<>(newState, entries);
                    });
        });
    }

    private WireCommands.TableKey convertToWireCommand(final TableSegmentKey k) {
        WireCommands.TableKey key;
        if (k.getVersion() == null) {
            // unconditional update.
            key = new WireCommands.TableKey(k.getKey(), WireCommands.TableKey.NO_VERSION);
        } else {
            key = new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion());
        }
        return key;
    }

    private TableSegmentEntry convertFromWireCommand(Map.Entry<WireCommands.TableKey, WireCommands.TableValue> e) {
        if (e.getKey().getKeyVersion() == WireCommands.TableKey.NOT_EXISTS) {
            return TableSegmentEntry.notExists(e.getKey().getData(), e.getValue().getData());
        } else {
            return TableSegmentEntry.versioned(e.getKey().getData(), e.getValue().getData(), e.getKey().getKeyVersion());
        }
    }
    
    private void closeConnection(Reply reply, RawClient client) {
        log.debug("Closing connection as a result of receiving: {}", reply);
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
    }

    private <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(RawClient connection, long requestId, T request) {
        return connection.sendRequest(requestId, request)
                         .exceptionally(e -> {
                             Throwable unwrap = Exceptions.unwrap(e);
                             if (unwrap instanceof ConnectionFailedException || unwrap instanceof ConnectionClosedException) {
                                 log.warn(requestId, "Connection dropped");
                                 throw StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, request.getType().name());
                             } else if (unwrap instanceof AuthenticationException) {
                                 log.warn(requestId, "Authentication Exception");
                                 throw StoreExceptions.create(StoreExceptions.Type.AUTH_ERROR, request.getType().name());
                             } else {
                                 log.error(requestId, "Request failed", e);
                                 throw new CompletionException(e);
                             }
                         });
    }

    /**
     * This method handle reply returned from RawClient.sendRequest.
     *  @param reply               actual reply received
     * @param client              RawClient for sending request
     * @param qualifiedStreamSegmentName StreamSegmentName
     * @param requestType         request which reply need to be transformed
     * @param type                Wire command Type
     */
    private void handleReply(Reply reply,
                             RawClient client,
                             String qualifiedStreamSegmentName,
                             Class<? extends Request> requestType,
                             WireCommandType type) {
        closeConnection(reply, client);
        Set<Class<? extends Reply>> expectedReplies = EXPECTED_SUCCESS_REPLIES.get(requestType);
        Set<Class<? extends Reply>> expectedFailingReplies = EXPECTED_FAILING_REPLIES.get(requestType);
        if (expectedReplies != null && expectedReplies.contains(reply.getClass())) {
            log.info(io.pravega.common.tracing.RequestTag.NON_EXISTENT_ID, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
        } else if (expectedFailingReplies != null && expectedFailingReplies.contains(reply.getClass())) {
            log.info(io.pravega.common.tracing.RequestTag.NON_EXISTENT_ID, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            if (reply instanceof WireCommands.NoSuchSegment) {
                throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, type.toString());
            } else if (reply instanceof WireCommands.TableSegmentNotEmpty) {
                throw StoreExceptions.create(StoreExceptions.Type.DATA_EXISTS, type.toString());
            } else if (reply instanceof WireCommands.TableKeyDoesNotExist) {
                throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, type.toString());
            } else if (reply instanceof WireCommands.TableKeyBadVersion) {
                throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, type.toString());
            }
        } else if (reply instanceof WireCommands.AuthTokenCheckFailed) {
            log.warn(io.pravega.common.tracing.RequestTag.NON_EXISTENT_ID, "Auth Check Failed {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            throw StoreExceptions.create(StoreExceptions.Type.AUTH_ERROR, type.toString());
        } else if (reply instanceof WireCommands.WrongHost) {
            log.warn(io.pravega.common.tracing.RequestTag.NON_EXISTENT_ID, "Wrong Host {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            throw StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, type.toString());
        } else {
            log.error(io.pravega.common.tracing.RequestTag.NON_EXISTENT_ID, "Unexpected reply {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());

            throw StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, 
                    new ConnectionFailedException("Unexpected reply of " + reply + " when expecting one of "
                    + expectedReplies.stream().map(Object::toString).collect(Collectors.joining(", "))), 
                            type.toString());
        }
    }
}
