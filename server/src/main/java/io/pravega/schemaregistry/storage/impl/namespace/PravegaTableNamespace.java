/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.namespace;

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

public class PravegaTableNamespace implements Namespace<Version> {
    static final String GROUPS_IN_NAMESPACE = PravegaTableNamespaces.SCHEMA_REGISTRY_SCOPE + "/GroupsInNamespace-%s-%s/0";
    private final TableStore tablesStore;
    private final ScheduledExecutorService executor;
    private final String namespace;
    private final String tableName;
    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    
    public PravegaTableNamespace(String namespace, String id, ClientConfig clientConfig, PravegaLogCache logCache,  
                                 TableStore tablesStore, ScheduledExecutorService executor) {
        this.tablesStore = tablesStore;
        this.executor = executor;
        this.tableName = String.format(GROUPS_IN_NAMESPACE, namespace, id);
        this.namespace = namespace;
        this.logCache = logCache;
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
        // 1. add entry to namespace table
        // 2. if entry already exists - if its state is creating only then proceed, else if it is created return
        // 3. create group object and call create on it. 
        // 4. update namespace entry to active
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
                                             groupProperties.isSubgroupBySchemaName(), groupProperties.getSchemaValidationRules()))
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
        // read from namespace's table
        return tablesStore.getAllKeys(tableName)
                          .thenApply(list -> new ListWithToken<>(list, null));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        // 1. mark group as deleting
        // 2. call group.delete
        //  2.1 delete index
        //  2.2 delete log
        //  2.3 delete the namespace entry
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
        PravegaLog log = new PravegaLog(namespace, groupName, value.getId(), clientConfig, logCache, executor);
        PravegaTableIndex index = new PravegaTableIndex(namespace, groupName, value.getId(), tablesStore);
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
