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

import io.pravega.schemaregistry.ListWithToken;

import java.util.concurrent.CompletableFuture;

public interface Namespaces<T> {
    CompletableFuture<ListWithToken<String>> getNamespaces();

    CompletableFuture<Void> addNewNamespace(String namespace);

    CompletableFuture<Void> removeNamespace(String namespace);

    CompletableFuture<Namespace<T>> getNamespace(String namespace);
}