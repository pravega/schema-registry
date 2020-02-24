/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.schemaregistry.ListWithToken;
import lombok.Synchronized;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class PravegaTableNamespaces implements Namespaces {
    public static final String NAMESPACES_TABLE = "namespaces";
    private final PravegaTablesStoreHelper tablesStoreHelper;
    private final ScheduledExecutorService executor;
    
    public PravegaTableNamespaces(PravegaTablesStoreHelper tablesStoreHelper, ScheduledExecutorService executor) {
        this.tablesStoreHelper = tablesStoreHelper;
        this.executor = executor;
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getNamespaces() {
        // read from namespaces table
        // if table does not exist create it. 
        List<String> namespaces = new LinkedList<>();
        return tablesStoreHelper.getAllKeys(NAMESPACES_TABLE).collectRemaining(namespaces::add)
                                .thenApply(v -> new ListWithToken<>(namespaces, null));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addNewNamespace(String namespace) {
        return Futures.toVoid(tablesStoreHelper.addNewEntryIfAbsent(NAMESPACES_TABLE, namespace, UUID.randomUUID().toString().getBytes()));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeNamespace(String namespace) {
        return Futures.toVoid(tablesStoreHelper.removeEntry(NAMESPACES_TABLE, namespace));
    }

    @Synchronized
    @Override
    public CompletableFuture<Namespace> getNamespace(String namespace) {
        return tablesStoreHelper.getCachedData(NAMESPACES_TABLE, namespace, String::new)
                         .thenApply(id -> new PravegaTableNamespace(id.getObject(), tablesStoreHelper, executor));
    }
}
