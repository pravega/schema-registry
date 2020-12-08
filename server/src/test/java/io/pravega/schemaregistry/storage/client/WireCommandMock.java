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

import io.netty.buffer.Unpooled;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.shared.protocol.netty.WireCommandType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class WireCommandMock {
    private static final int SERVICE_PORT = 12345;
    
    public static WireCommandClient getMock(ScheduledExecutorService executor) {
        final Object lock = new Object();
        final Map<String, Map<ByteBuffer, TableSegmentEntry>> mapOfTables = new HashMap<>();
        final Map<String, Map<ByteBuffer, Long>> mapOfTablesPosition = new HashMap<>();
        WireCommandClient helper = spy(new WireCommandClient(mock(ConnectionPool.class), mock(HostStore.class)));

        HostStore hoststore = mock(HostStore.class);
        
        doReturn(CompletableFuture.completedFuture(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()))
                .when(hoststore).getHostForTableSegment(anyString());

        ControllerImpl controller = mock(ControllerImpl.class);
        doReturn(CompletableFuture.completedFuture("")).when(controller).getOrRefreshDelegationTokenFor(anyString(), anyString(), any());
        doReturn(CompletableFuture.completedFuture(true)).when(controller).createScope(anyString());

        doReturn(controller).when(hoststore).getController();
        doReturn(hoststore).when(helper).getHostStore();
        
        doReturn(CompletableFuture.completedFuture(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()))
                .when(helper).getTableUri(anyString());

        // region create table
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    mapOfTables.putIfAbsent(tableName, new HashMap<>());
                    mapOfTablesPosition.putIfAbsent(tableName, new HashMap<>());
                }
            }, executor);
        }).when(helper).createTableSegment(anyString(), anyString());
        // endregion
        
        // region delete table
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            Boolean mustBeEmpty = x.getArgument(1);
            final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    if (!mapOfTables.containsKey(tableName)) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "a");
                    }
                    boolean empty = Optional.ofNullable(mapOfTables.get(tableName)).orElse(Collections.emptyMap()).isEmpty();
                    if (!mustBeEmpty || empty) {
                        mapOfTables.remove(tableName);
                        mapOfTablesPosition.remove(tableName);
                        return null;
                    } else {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_EMPTY, "A");
                    }
                }
            }, executor);
        }).when(helper).deleteTableSegment(anyString(), anyBoolean(), anyString());
        // endregion
        
        // region update keys
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
            String tableName = x.getArgument(0);
            List<TableSegmentEntry> entries = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "A");
                    } else {
                        List<TableSegmentKeyVersion> resultList = new LinkedList<>();
                        entries.forEach(entry -> {
                            ByteBuffer key = entry.getKey().getKey().copy().nioBuffer();
                            byte[] value = entry.getValue().copy().array();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry == null) {
                                if (entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                                    TableSegmentEntry newEntry = TableSegmentEntry.versioned(key.array(), value, 0);
                                    table.put(key, newEntry);
                                    tablePos.put(key, System.nanoTime());
                                    resultList.add(newEntry.getKey().getVersion());
                                } else {
                                    throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "A");
                                }
                            } else if (existingEntry.getKey().getVersion().equals(entry.getKey().getVersion())) {
                                TableSegmentKeyVersion newVersion = TableSegmentKeyVersion.from(
                                        existingEntry.getKey().getVersion().getSegmentVersion() + 1);
                                TableSegmentEntry newEntry = TableSegmentEntry.versioned(key.array(), value, newVersion.getSegmentVersion());
                                table.put(key, newEntry);
                                tablePos.put(key, System.nanoTime());
                                resultList.add(newVersion);
                            } else {
                                throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "A");
                            }
                        });
                        return resultList;
                    }
                }
            }, executor);
        }).when(helper).updateTableEntries(anyString(), any(), anyString());
        // endregion
    
        // region remove keys    
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
            String tableName = x.getArgument(0);
            List<TableSegmentKey> keys = x.getArgument(1);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "A");
                    } else {
                        keys.forEach(rawKey -> {
                            ByteBuffer key = rawKey.getKey().copy().nioBuffer();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry != null) {
                                if (existingEntry.getKey().getVersion().equals(rawKey.getVersion())
                                        || rawKey.getVersion() == null
                                        || rawKey.getVersion().equals(TableSegmentKeyVersion.NO_VERSION)) {
                                    table.remove(key);
                                    tablePos.remove(key);
                                } else {
                                    throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "A");
                                }
                            }
                        });
                    }
                }
            }, executor);
        }).when(helper).removeTableKeys(anyString(), any(), anyString());
        // endregion

        // region read keys    
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.READ_TABLE;
            String tableName = x.getArgument(0);
            List<TableSegmentKey> requestKeys = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    if (table == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "A");
                    } else {
                        List<TableSegmentEntry> resultList = new LinkedList<>();

                        requestKeys.forEach(requestKey -> {
                            ByteBuffer key = requestKey.getKey().copy().nioBuffer();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry == null) {
                                resultList.add(TableSegmentEntry.notExists(new byte[1], new byte[1]));
                            } else if (existingEntry.getKey().getVersion().equals(requestKey.getVersion())
                                    || requestKey.getVersion() == null
                                    || requestKey.getVersion().equals(TableSegmentKeyVersion.NO_VERSION)) {
                                resultList.add(duplicate(existingEntry));
                            } else {
                                throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "A");
                            }
                        });

                        return resultList;
                    }
                }
            }, executor);
        }).when(helper).readTable(anyString(), any(), anyString());
        // endregion
        
        // region readTableKeys
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            int limit = x.getArgument(1);
            IteratorState state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "A");
                    } else {
                        long floor;
                        if (state.equals(IteratorStateImpl.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = BitConverter.readLong(state.toBytes().array(), 0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableSegmentKey> list = tablePos.entrySet().stream()
                                                             .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                             .filter(c -> c.getValue() > floor)
                                                             .map(r -> {
                                                                 token.set(r.getValue());
                                                                 return duplicate(table.get(r.getKey()).getKey());
                                                             })
                                                             .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        IteratorState newState = IteratorStateImpl.fromBytes(Unpooled.wrappedBuffer(continuationToken));
                        return new IteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableKeys(anyString(), anyInt(), any(), anyString());
        // endregion        
        
        // region readTableEntries
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            int limit = x.getArgument(1);
            IteratorState state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_CONTAINER_NOT_FOUND, "A");
                    } else {
                        long floor;
                        if (state.equals(IteratorStateImpl.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = BitConverter.readLong(state.toBytes().array(), 0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableSegmentEntry> list = tablePos.entrySet().stream()
                                                               .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                               .filter(c -> c.getValue() > floor)
                                                               .map(r -> {
                                                                   token.set(r.getValue());
                                                                   return duplicate(table.get(r.getKey()));
                                                               })
                                                               .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        IteratorState newState = IteratorStateImpl.fromBytes(Unpooled.wrappedBuffer(continuationToken));
                        return new IteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableEntries(anyString(), anyInt(), any(), anyString());
        // endregion
        
        return helper;
    }

    private static TableSegmentKey duplicate(TableSegmentKey key) {
        return key.getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)
                ? TableSegmentKey.notExists(key.getKey().copy())
                : TableSegmentKey.versioned(key.getKey().copy(), key.getVersion().getSegmentVersion());
    }

    private static TableSegmentEntry duplicate(TableSegmentEntry entry) {
        return entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)
                ? TableSegmentEntry.notExists(entry.getKey().getKey().copy(), entry.getValue().copy())
                : TableSegmentEntry.versioned(entry.getKey().getKey().copy(), entry.getValue().copy(), entry.getKey().getVersion().getSegmentVersion());
    }
}
