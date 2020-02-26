/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.namespace;

import io.pravega.client.ClientConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.Version;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.impl.group.PravegaLogCache;
import lombok.Synchronized;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * Pravega tables based Namespaces implementation. It stores all namespaces in a table identified by
 * {@link PravegaTableNamespaces#NAMESPACES_TABLE}. 
 * Each entry represents a namespace. Once a namespace 
 */
public class PravegaTableNamespaces implements Namespaces<Version> {
    public static final String SCHEMA_REGISTRY_SCOPE = "pravega-schema-registry";
    public static final String NAMESPACES_TABLE = SCHEMA_REGISTRY_SCOPE + "/namespaces/0";
    
    private final TableStore tableStore;
    private final ScheduledExecutorService executor;
    private final PravegaLogCache logCache;
    private final ClientConfig clientConfig;
    
    public PravegaTableNamespaces(ClientConfig clientConfig, TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
        this.logCache = new PravegaLogCache(clientConfig);
        this.clientConfig = clientConfig;
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getNamespaces() {
        // read from namespaces table
        // if table does not exist create it. 
        List<String> namespaces = new LinkedList<>();
        return withCreateNamespacesTableIfNotExist(() -> tableStore.getAllKeys(NAMESPACES_TABLE).collectRemaining(namespaces::add))
                      .thenApply(v -> new ListWithToken<>(namespaces, null));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addNewNamespace(String namespace) {
        String id = UUID.randomUUID().toString();
        IdWithState idWithState = new IdWithState(id, IdWithState.State.Creating);
        return withCreateNamespacesTableIfNotExist(() -> 
                tableStore.addNewEntryIfAbsent(NAMESPACES_TABLE, namespace, idWithState.toBytes()))
                .thenCompose(v -> tableStore.getEntry(NAMESPACES_TABLE, namespace, IdWithState::fromBytes))
                .thenCompose(entry -> {
                    if (entry.getObject().getState().equals(IdWithState.State.Creating)) {
                        String idInStore = entry.getObject().getId();
                        String tableName = String.format(PravegaTableNamespace.GROUPS_IN_NAMESPACE, namespace, idInStore);
                        IdWithState newIdWithState = new IdWithState(idInStore, IdWithState.State.Active);
                        return Futures.toVoid(tableStore.createTable(tableName)
                                         .thenCompose(v -> tableStore.updateEntry(NAMESPACES_TABLE, namespace, newIdWithState.toBytes(), entry.getVersion())));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeNamespace(String namespace) {
        return tableStore.getEntry(NAMESPACES_TABLE, namespace, IdWithState::fromBytes)
                          .thenCompose(entry -> {
                              IdWithState newValue = new IdWithState(entry.getObject().getId(), IdWithState.State.Deleting);
                              String tableName = String.format(PravegaTableNamespace.GROUPS_IN_NAMESPACE, namespace, entry.getObject().getId());

                              return tableStore.updateEntry(NAMESPACES_TABLE, namespace, newValue.toBytes(), entry.getVersion())
                                                .thenCompose(version -> tableStore.deleteTable(tableName, false));
                          }).thenCompose(v -> tableStore.removeEntry(NAMESPACES_TABLE, namespace));
    }

    @Synchronized
    @Override
    public CompletableFuture<Namespace<Version>> getNamespace(String namespace) {
        return tableStore.getEntry(NAMESPACES_TABLE, namespace, IdWithState::fromBytes)
                         .thenCompose(entry -> {
                             CompletableFuture<Void> future;
                             if (entry.getObject().getState().equals(IdWithState.State.Creating)) {
                                 future = addNewNamespace(namespace);
                             } else {
                                 future = CompletableFuture.completedFuture(null);
                             }
                             return future.thenApply(v -> new PravegaTableNamespace(namespace, entry.getObject().getId(),
                                     clientConfig, logCache, tableStore, executor));
                         });
    }
    
    private <T> CompletableFuture<T> withCreateNamespacesTableIfNotExist(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                () -> tableStore.createTable(NAMESPACES_TABLE).thenCompose(v -> supplier.get()));
    }
}
