/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.groups;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PravegaKVGroups implements Groups<Version> {
    private static final String GROUPS = TableStore.SCHEMA_REGISTRY_SCOPE + "/groups/0";

    private final TableStore tableStore;
    private final ScheduledExecutorService executor;

    public PravegaKVGroups(TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Group<Version>> getGroup(String namespace, String groupName) {
        return withCreateGroupsTableIfAbsent(() -> tableStore.getEntry(GROUPS, 
                new NamespaceAndGroup(namespace, groupName).toBytes(), GroupsValue::fromBytes))
                .thenApply(entry -> {
                    if (!entry.getRecord().getState().equals(GroupsValue.State.Active)) {
                        throw new IllegalStateException();
                    } else {
                        return getGroupObject(entry.getRecord()).getGroup();
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> addNewGroup(String namespace, String groupName, GroupProperties groupProperties) {
        // 1. add entry to groups table
        // 2. if entry already exists - if its state is creating only then proceed, else if it is created return
        // 3. create group object and call create on it. 
        // 4. update groups entry to active
        String id = UUID.randomUUID().toString();
        GroupsValue value = new GroupsValue(id, GroupsValue.State.Creating);
        byte[] key = new NamespaceAndGroup(namespace, groupName).toBytes();
        return withCreateGroupsTableIfAbsent(() -> tableStore.addNewEntryIfAbsent(GROUPS, key, value.toBytes()))
                .thenCompose(v -> tableStore.getEntry(GROUPS, key, GroupsValue::fromBytes))
                .thenCompose(entry -> {
                    if (entry.getRecord().getState().equals(GroupsValue.State.Creating)) {
                        GroupObj groupObject = getGroupObject(entry.getRecord());
                        Group<Version> grp = groupObject.getGroup();
                        PravegaKVGroupTable index = groupObject.getIndex();

                        boolean toReturn = entry.getRecord().getId().equals(id);
                        return index.create()
                                    .thenCompose(v -> grp.create(groupProperties.getSerializationFormat(), groupProperties.getProperties(),
                                            groupProperties.isAllowMultipleTypes(), groupProperties.getSchemaValidationRules()))
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
    public CompletableFuture<ListWithToken<String>> getGroups(String nameSpace, ContinuationToken token, int limit) {
        String namespace = nameSpace == null ? "" : nameSpace;
        ByteBuf buffer;
        if (token == null || token.equals(ContinuationToken.EMPTY)) {
            buffer = IteratorState.EMPTY.toBytes();
        } else {
            byte[] bytes = Base64.getDecoder().decode(token.toString());
            buffer = Unpooled.wrappedBuffer(bytes);
        }
        return withCreateGroupsTableIfAbsent(() -> tableStore.getKeysPaginated(GROUPS, buffer, limit, NamespaceAndGroup::fromBytes))
                .thenApply(resp -> {
                    List<String> groups = resp.getValue().stream()
                                             .filter(x -> x.getNamespace().equals(namespace))
                                             .map(NamespaceAndGroup::getGroupId)
                                             .collect(Collectors.toList());
                    return new ListWithToken<>(groups, ContinuationToken.create(Base64.getEncoder().encodeToString(resp.getKey().array())));
                });
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String groupName) {
        // 1. mark group as deleting
        // 2. call group.delete
        //  2.1 delete index
        //  2.2 delete log
        // 3. delete the groups entry
        byte[] key = new NamespaceAndGroup(namespace, groupName).toBytes();
        return Futures.exceptionallyExpecting(tableStore.getEntry(GROUPS, key, GroupsValue::fromBytes)
                                                        .thenCompose(entry -> {
                                                            GroupsValue newValue = new GroupsValue(entry.getRecord().getId(), GroupsValue.State.Deleting);
                                                            return tableStore.updateEntry(GROUPS, key, newValue.toBytes(), entry.getVersion())
                                                                             .thenCompose(version -> {
                                                                                 GroupObj grpObj = getGroupObject(newValue);
                                                                                 return grpObj.getIndex().delete();
                                                                             });
                                                        })
                                                        .thenCompose(v -> tableStore.removeEntry(GROUPS, key)),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
    }

    private GroupObj getGroupObject(GroupsValue value) {
        PravegaKVGroupTable index = new PravegaKVGroupTable(value.getId(), tableStore);
        Group<Version> group = new Group<>(index, executor);
        return new GroupObj(group, index);
    }

    private <T> CompletableFuture<T> withCreateGroupsTableIfAbsent(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException,
                () -> tableStore.createTable(GROUPS).thenCompose(v -> supplier.get()));
    }

    @Data
    private static class GroupObj {
        private final Group<Version> group;
        private final PravegaKVGroupTable index;
    }
}
