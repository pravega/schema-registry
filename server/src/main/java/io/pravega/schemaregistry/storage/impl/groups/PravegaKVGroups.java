/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.groups;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.common.FuturesCollector;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.PravegaKVGroupTable;
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;
import lombok.Data;
import lombok.SneakyThrows;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PravegaKVGroups implements Groups<Version> {
    public static final String GROUPS = TableStore.SCHEMA_REGISTRY_SCOPE + "/groups/0";

    private final TableStore tableStore;
    private final ScheduledExecutorService executor;

    public PravegaKVGroups(TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Group<Version>> getGroup(String namespace, String group) {
        return withCreateGroupsTableIfAbsent(() -> tableStore.getEntry(GROUPS, 
                new NamespaceAndGroup(namespace, group).toBytes(), GroupsValue::fromBytes))
                .thenCompose(entry -> {
                    if (entry.getRecord().getState().equals(GroupsValue.State.Creating)) {
                        // if a group is in creating state, we will throw data not found exception as this group is not 
                        // available for any action. 
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "group not created yet.");
                    } else if (entry.getRecord().getState().equals(GroupsValue.State.Deleting)) {
                        // if a get group request is made for a deleting group, we will delete it and throw data not found.
                        return deleteGroup(namespace, group)
                                .thenApply(v -> {
                                    throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "group not found.");
                                });
                    } else {
                        return CompletableFuture.completedFuture(getGroupObject(entry.getRecord()).getGroup());
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> addNewGroup(String namespace, String group, GroupProperties groupProperties) {
        // 1. add entry to groups table
        // 2. if group is already present and active, return false. If group entry is either created with state = creating
        // or was already present but the state was creating, proceed with creation of group metadata.
        // 3. create group metadata by calling `create` method on the group object. 
        // 4. update groups entry to active
        String id = UUID.randomUUID().toString();
        GroupsValue value = new GroupsValue(id, GroupsValue.State.Creating);
        byte[] key = new NamespaceAndGroup(namespace, group).toBytes();
        return withCreateGroupsTableIfAbsent(() -> tableStore.addNewEntryIfAbsent(GROUPS, key, value.toBytes()))
                .thenCompose(v -> tableStore.getEntry(GROUPS, key, GroupsValue::fromBytes))
                .thenCompose(entry -> {
                    if (entry.getRecord().getState().equals(GroupsValue.State.Creating)) {
                        GroupObj groupObject = getGroupObject(entry.getRecord());
                        Group<Version> grp = groupObject.getGroup();
                        PravegaKVGroupTable index = groupObject.getGroupTable();

                        boolean toReturn = entry.getRecord().getId().equals(id);
                        return index.create()
                                    .thenCompose(v -> grp.create(groupProperties.getSerializationFormat(), groupProperties.getProperties(),
                                            groupProperties.isAllowMultipleTypes(), groupProperties.getCompatibility()))
                                    .thenCompose(v -> {
                                        byte[] newValue = new GroupsValue(entry.getRecord().getId(), GroupsValue.State.Active).toBytes();
                                        return tableStore.updateEntry(GROUPS, key, newValue, entry.getVersion());
                                    })
                                    .thenApply(v -> toReturn);
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                });
    }

    @SneakyThrows
    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(String nameSpace, ContinuationToken token, int limit) {
        String namespace = nameSpace == null ? "" : nameSpace;
        ByteBuf continuationToken;
        if (token == null || token.equals(ContinuationToken.EMPTY)) {
            continuationToken = IteratorState.EMPTY.toBytes();
        } else {
            byte[] bytes = Base64.getDecoder().decode(token.toString());
            continuationToken = Unpooled.wrappedBuffer(bytes);
        }
        BiFunction<ByteBuf, Integer, CompletableFuture<Map.Entry<ByteBuf, List<NamespaceAndGroup>>>> function = 
                (ByteBuf t, Integer l) -> withCreateGroupsTableIfAbsent(
                        () -> tableStore.getKeysPaginated(GROUPS, t, l, NamespaceAndGroup::fromBytes));
        Predicate<NamespaceAndGroup> predicate = x -> x.getNamespace().equals(namespace);
        return FuturesCollector.filteredWithTokenAndLimit(function, predicate, continuationToken, limit, executor)
                               .thenApply(result -> {
                                             List<String> groups = result.getValue().stream().map(NamespaceAndGroup::getGroupId).collect(Collectors.toList());
                                             ContinuationToken continuationTok = ContinuationToken.create(Base64.getEncoder().encodeToString(result.getKey().array()));
                                             return new ListWithToken<>(groups, continuationTok);
                                         });
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        // 1. if group state is "active" or "deleting", mark group entry in groups table as deleting.
        // 2. call group.delete
        // 3. delete the entry in groups table
        byte[] key = new NamespaceAndGroup(namespace, group).toBytes();
        return Futures.exceptionallyExpecting(
                tableStore.getEntry(GROUPS, key, GroupsValue::fromBytes)
                          .thenCompose(entry -> {
                              if (!entry.getRecord().getState().equals(GroupsValue.State.Creating)) {
                                  GroupsValue newValue = new GroupsValue(entry.getRecord().getId(), GroupsValue.State.Deleting);
                                  return tableStore.updateEntry(GROUPS, key, newValue.toBytes(), entry.getVersion())
                                                   .thenCompose(version -> {
                                                       GroupObj grpObj = getGroupObject(newValue);
                                                       return grpObj.getGroupTable().delete()
                                                                    .thenCompose(v -> tableStore.removeEntry(GROUPS, key));
                                                   });
                              } else {
                                  return CompletableFuture.completedFuture(null);
                              }
                          }),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
    }

    private GroupObj getGroupObject(GroupsValue value) {
        PravegaKVGroupTable groupTable = new PravegaKVGroupTable(value.getId(), tableStore);
        Group<Version> group = new Group<>(groupTable, executor);
        return new GroupObj(group, groupTable);
    }

    private <T> CompletableFuture<T> withCreateGroupsTableIfAbsent(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException,
                () -> tableStore.createTable(GROUPS).thenCompose(v -> supplier.get()));
    }

    @Data
    private static class GroupObj {
        private final Group<Version> group;
        private final PravegaKVGroupTable groupTable;
    }
}
