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

import io.pravega.client.ClientConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.PravegaLog;
import io.pravega.schemaregistry.storage.impl.group.PravegaLogCache;
import io.pravega.schemaregistry.storage.impl.group.PravegaTableIndex;
import lombok.Data;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class PravegaTableGroups implements Groups<Version> {
    private static final String GROUPS = "schema-registry/groups/0";

    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    private final TableStore tableStore;
    private final ScheduledExecutorService executor;
    private final String tableName;
    
    public PravegaTableGroups(ClientConfig clientConfig, TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
        this.tableName = GROUPS;
        this.logCache = new PravegaLogCache(clientConfig);
        this.clientConfig = clientConfig;
    }
    
    @Override
    public CompletableFuture<Group<Version>> getGroup(String groupName) {
        return withCreateGroupsTableIfAbsent(() -> tableStore.getEntry(tableName, groupName.getBytes(Charsets.UTF_8), GroupsValue::fromBytes))
                         .thenApply(entry -> {
                       if (!entry.getObject().getState().equals(GroupsValue.State.Active)) {
                           throw new IllegalStateException();
                       } else {
                           return getGroupObject(groupName, entry.getObject()).getGroup();
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
        return withCreateGroupsTableIfAbsent(() -> tableStore.addNewEntryIfAbsent(tableName, group.getBytes(Charsets.UTF_8), value.toBytes()))
                         .thenCompose(v -> tableStore.getEntry(tableName, group.getBytes(Charsets.UTF_8), GroupsValue::fromBytes))
                         .thenCompose(entry -> {
                       if (entry.getObject().getState().equals(GroupsValue.State.Creating)) {
                           GroupObj groupObject = getGroupObject(group, entry.getObject());
                           Group<Version> grp = groupObject.getGroup();
                           PravegaLog log = groupObject.getLog();
                           PravegaTableIndex index = groupObject.getIndex();
                           
                           boolean toReturn = entry.getObject().getId().equals(id);
                           return log.create().thenCompose(v -> index.create())
                                     .thenCompose(v -> grp.create(groupProperties.getSchemaType(), groupProperties.getProperties(),
                                             groupProperties.isValidateByObjectType(), groupProperties.getSchemaValidationRules()))
                                     .thenCompose(v -> {
                                         byte[] newValue = new GroupsValue(entry.getObject().getId(), GroupsValue.State.Active).toBytes();
                                         return tableStore.updateEntry(tableName, group.getBytes(Charsets.UTF_8), newValue, entry.getVersion());
                                     })
                              .thenApply(v -> toReturn);
                       } else {
                           return CompletableFuture.completedFuture(false);
                       }
                   });
    }

    @Override
    public CompletableFuture<ListWithToken<String>> getGroups() {
        // read from groups's table
        return withCreateGroupsTableIfAbsent(() -> tableStore.getAllKeys(tableName, x -> new String(x, Charsets.UTF_8)))
                         .thenApply(list -> new ListWithToken<>(list, null));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        // 1. mark group as deleting
        // 2. call group.delete
        //  2.1 delete index
        //  2.2 delete log
        // 3. delete the groups entry
        return Futures.exceptionallyExpecting(tableStore.getEntry(tableName, group.getBytes(Charsets.UTF_8), GroupsValue::fromBytes)
                         .thenCompose(entry -> {
                              GroupsValue newValue = new GroupsValue(entry.getObject().getId(), GroupsValue.State.Deleting);
                              return tableStore.updateEntry(tableName, group.getBytes(Charsets.UTF_8), newValue.toBytes(), entry.getVersion())
                                               .thenCompose(version -> {
                                                    GroupObj grpObj = getGroupObject(group, newValue);
                                                    return CompletableFuture.allOf(grpObj.getLog().delete(), grpObj.getIndex().delete());
                                                });
                          })
                         .thenCompose(v -> tableStore.removeEntry(tableName, group.getBytes(Charsets.UTF_8))), 
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
    }

    private GroupObj getGroupObject(String groupName, GroupsValue value) {
        PravegaLog log = new PravegaLog(groupName, value.getId(), clientConfig, logCache, executor);
        PravegaTableIndex index = new PravegaTableIndex(groupName, value.getId(), tableStore);
        Group<Version> group = new Group<>(log, index, executor);
        return new GroupObj(group, log, index);
    }

    private <T> CompletableFuture<T> withCreateGroupsTableIfAbsent(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                () -> tableStore.createTable(GROUPS).thenCompose(v -> supplier.get()));
    }
    
    @Data
    private static class GroupObj {
        private final Group<Version> group;
        private final PravegaLog log;
        private final PravegaTableIndex index;
    } 
}
