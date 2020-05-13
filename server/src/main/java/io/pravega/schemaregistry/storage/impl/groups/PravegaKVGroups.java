/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
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
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class PravegaKVGroups implements Groups<Version> {
    private static final String GROUPS = "schema-registry/groups/0";

    private final TableStore tableStore;
    private final ScheduledExecutorService executor;

    public PravegaKVGroups(TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Group<Version>> getGroup(String groupName) {
        return withCreateGroupsTableIfAbsent(() -> tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8), GroupsValue::fromBytes))
                .thenApply(entry -> {
                    if (!entry.getRecord().getState().equals(GroupsValue.State.Active)) {
                        throw new IllegalStateException();
                    } else {
                        return getGroupObject(groupName, entry.getRecord()).getGroup();
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> addNewGroup(String group, GroupProperties groupProperties) {
        // 1. add entry to groups table
        // 2. if entry already exists - if its state is creating only then proceed, else if it is created return
        // 3. create group object and call create on it. 
        // 4. update groups entry to active
        String id = UUID.randomUUID().toString();
        GroupsValue value = new GroupsValue(id, GroupsValue.State.Creating);
        return withCreateGroupsTableIfAbsent(() -> tableStore.addNewEntryIfAbsent(GROUPS, group.getBytes(Charsets.UTF_8), value.toBytes()))
                .thenCompose(v -> tableStore.getEntry(GROUPS, group.getBytes(Charsets.UTF_8), GroupsValue::fromBytes))
                .thenCompose(entry -> {
                    if (entry.getRecord().getState().equals(GroupsValue.State.Creating)) {
                        GroupObj groupObject = getGroupObject(group, entry.getRecord());
                        Group<Version> grp = groupObject.getGroup();
                        PravegaKVGroupTable index = groupObject.getIndex();

                        boolean toReturn = entry.getRecord().getId().equals(id);
                        return index.create()
                                    .thenCompose(v -> grp.create(groupProperties.getSchemaType(), groupProperties.getProperties(),
                                            groupProperties.isVersionBySchemaName(), groupProperties.getSchemaValidationRules()))
                                    .thenCompose(v -> {
                                        byte[] newValue = new GroupsValue(entry.getRecord().getId(), GroupsValue.State.Active).toBytes();
                                        return tableStore.updateEntry(GROUPS, group.getBytes(Charsets.UTF_8), newValue, entry.getVersion());
                                    })
                                    .thenApply(v -> toReturn);
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                });
    }

    @SneakyThrows
    @Override
    public CompletableFuture<ListWithToken<String>> getGroups(ContinuationToken token, int limit) {
        ByteBuf buffer;
        if (token == null || token.equals(ContinuationToken.EMPTY)) {
            buffer = IteratorState.EMPTY.toBytes();
        } else {
            byte[] bytes = Base64.getDecoder().decode(token.toString());
            buffer = Unpooled.wrappedBuffer(bytes);
        }
        return withCreateGroupsTableIfAbsent(() -> tableStore.getKeysPaginated(GROUPS, buffer, limit, x -> new String(x, Charsets.UTF_8)))
                .thenApply(resp -> new ListWithToken<>(resp.getValue(), ContinuationToken.create(Base64.getEncoder().encodeToString(resp.getKey().array()))));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        // 1. mark group as deleting
        // 2. call group.delete
        //  2.1 delete index
        //  2.2 delete log
        // 3. delete the groups entry
        return Futures.exceptionallyExpecting(tableStore.getEntry(GROUPS, group.getBytes(Charsets.UTF_8), GroupsValue::fromBytes)
                                                        .thenCompose(entry -> {
                                                            GroupsValue newValue = new GroupsValue(entry.getRecord().getId(), GroupsValue.State.Deleting);
                                                            return tableStore.updateEntry(GROUPS, group.getBytes(Charsets.UTF_8), newValue.toBytes(), entry.getVersion())
                                                                             .thenCompose(version -> {
                                                                                 GroupObj grpObj = getGroupObject(group, newValue);
                                                                                 return grpObj.getIndex().delete();
                                                                             });
                                                        })
                                                        .thenCompose(v -> tableStore.removeEntry(GROUPS, group.getBytes(Charsets.UTF_8))),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
    }

    private GroupObj getGroupObject(String groupName, GroupsValue value) {
        PravegaKVGroupTable index = new PravegaKVGroupTable(groupName, value.getId(), tableStore);
        Group<Version> group = new Group<>(groupName, index, executor);
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
