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

import io.pravega.schemaregistry.ListWithToken;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class InMemoryNamespaces implements Namespaces {
    @GuardedBy("$lock")
    private final Map<String, Namespace> scopes = new HashMap<>();
    private final ScheduledExecutorService executor;

    public InMemoryNamespaces(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getNamespaces() {
        List<String> list = scopes.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        return CompletableFuture.completedFuture(new ListWithToken<>(list, null));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addNewNamespace(String scope) {
        scopes.putIfAbsent(scope, new InMemoryNamespace(executor));
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeNamespace(String scope) {
        scopes.remove(scope);
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    @Override
    public CompletableFuture<Namespace> getNamespace(String scope) {
        return CompletableFuture.completedFuture(scopes.get(scope));
    }
}
