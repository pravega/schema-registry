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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.storage.client.TableStore;
import lombok.Synchronized;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class PravegaTableNamespaces implements Namespaces {
    public static final String NAMESPACES_TABLE = "namespaces";
    private final TableStore tableStore;
    private final ScheduledExecutorService executor;
    
    public PravegaTableNamespaces(TableStore tableStore, ScheduledExecutorService executor) {
        this.tableStore = tableStore;
        this.executor = executor;
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getNamespaces() {
        // read from namespaces table
        // if table does not exist create it. 
        List<String> namespaces = new LinkedList<>();
        return withCreateTableIfNotExist(() -> tableStore.getAllKeys(NAMESPACES_TABLE).collectRemaining(namespaces::add))
                      .thenApply(v -> new ListWithToken<>(namespaces, null));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addNewNamespace(String namespace) {
        return withCreateTableIfNotExist(() -> Futures.toVoid(
                tableStore.addNewEntryIfAbsent(NAMESPACES_TABLE, namespace, UUID.randomUUID().toString().getBytes())));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeNamespace(String namespace) {
        return withCreateTableIfNotExist(() -> Futures.toVoid(tableStore.removeEntry(NAMESPACES_TABLE, namespace)));
    }

    @Synchronized
    @Override
    public CompletableFuture<Namespace> getNamespace(String namespace) {
        return withCreateTableIfNotExist(() -> tableStore.getCachedData(NAMESPACES_TABLE, namespace, String::new))
                          .thenApply(id -> new PravegaTableNamespace(id.getObject(), tableStore, executor))
                .thenCompose(ns -> ns.checkNamespaceExist()
                        .thenCompose(exists -> {
                            if (exists) {
                                return CompletableFuture.completedFuture(ns);
                            } else {
                                tableStore.invalidateCache(NAMESPACES_TABLE, namespace);
                                return tableStore.getCachedData(NAMESPACES_TABLE, namespace, String::new)
                                                 .thenApply(id -> new PravegaTableNamespace(id.getObject(), tableStore, executor));
                            }
                        }));
    }
    
    private <T> CompletableFuture<T> withCreateTableIfNotExist(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                () -> tableStore.createTable(NAMESPACES_TABLE).thenCompose(v -> supplier.get()));
    }
}
