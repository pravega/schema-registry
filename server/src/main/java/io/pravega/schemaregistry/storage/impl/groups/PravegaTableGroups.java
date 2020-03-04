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
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.PravegaLogCache;
import io.pravega.schemaregistry.storage.impl.group.PravegaLog;
import io.pravega.schemaregistry.storage.impl.group.PravegaTableIndex;
import lombok.Data;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaTableGroups implements Groups<Version> {
    public static final String SCHEMA_REGISTRY_SCOPE = "pravega-schema-registry";
    public static final String GROUPS = SCHEMA_REGISTRY_SCOPE + "/groups/0";

    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    private final TableStore tablesStore;
    private final ScheduledExecutorService executor;
    private final String tableName;
    
    public PravegaTableGroups(ClientConfig clientConfig, TableStore tablesStore, ScheduledExecutorService executor) {
        this.tablesStore = tablesStore;
        this.executor = executor;
        this.tableName = GROUPS;
        this.logCache = new PravegaLogCache(clientConfig);
        this.clientConfig = clientConfig;
    }
    
    @Override
    public CompletableFuture<Group<Version>> getGroup(String groupName) {
        return tablesStore.getEntry(tableName, groupName, IdWithState::fromBytes)
                   .thenApply(entry -> {
                       if (!entry.getObject().getState().equals(IdWithState.State.Active)) {
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
        IdWithState value = new IdWithState(id, IdWithState.State.Creating);
        return tablesStore.addNewEntryIfAbsent(tableName, group, value.toBytes())
                   .thenCompose(v -> tablesStore.getEntry(tableName, group, IdWithState::fromBytes))
                   .thenCompose(entry -> {
                       if (entry.getObject().getState().equals(IdWithState.State.Creating)) {
                           GroupObj groupObject = getGroupObject(group, entry.getObject());
                           Group<Version> grp = groupObject.getGroup();
                           PravegaLog log = groupObject.getLog();
                           PravegaTableIndex index = groupObject.getIndex();
                           
                           boolean toReturn = entry.getObject().getId().equals(id);
                           return log.create().thenCompose(v -> index.create())
                                     .thenCompose(v -> grp.create(groupProperties.getSchemaType(), groupProperties.isEnableEncoding(),
                                             groupProperties.isValidateByObjectType(), groupProperties.getSchemaValidationRules()))
                                     .thenCompose(v -> {
                                         byte[] newValue = new IdWithState(entry.getObject().getId(), IdWithState.State.Active).toBytes();
                                         return tablesStore.updateEntry(tableName, group, newValue, entry.getVersion());
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
        return tablesStore.getAllKeys(tableName)
                          .thenApply(list -> new ListWithToken<>(list, null));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        // 1. mark group as deleting
        // 2. call group.delete
        //  2.1 delete index
        //  2.2 delete log
        //  2.3 delete the groups entry
        return tablesStore.getEntry(tableName, group, IdWithState::fromBytes)
                          .thenCompose(entry -> {
                              IdWithState newValue = new IdWithState(entry.getObject().getId(), IdWithState.State.Deleting);
                              return tablesStore.updateEntry(tableName, group, newValue.toBytes(), entry.getVersion())
                                                .thenCompose(version -> {
                                                    GroupObj grpObj = getGroupObject(group, newValue);
                                                    return CompletableFuture.allOf(grpObj.getLog().delete(), grpObj.getIndex().delete());
                                                });
                          })
                .thenCompose(v -> tablesStore.removeEntry(tableName, group));
    }

    private GroupObj getGroupObject(String groupName, IdWithState value) {
        PravegaLog log = new PravegaLog(groupName, value.getId(), clientConfig, logCache, executor);
        PravegaTableIndex index = new PravegaTableIndex(groupName, value.getId(), tablesStore);
        Group<Version> group = new Group<>(log, index, executor);
        return new GroupObj(group, log, index);
    }

    @Data
    private static class GroupObj {
        private final Group<Version> group;
        private final PravegaLog log;
        private final PravegaTableIndex index;
    } 
}
