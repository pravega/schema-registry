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
import java.util.stream.Collectors;

public class InMemoryScopes implements Scopes {
    @GuardedBy("$lock")
    private final Map<String, Scope> scopes = new HashMap<>();
    
    @Synchronized
    @Override
    public ListWithToken<String> getScopes() {
        List<String> list = scopes.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        return new ListWithToken<>(list, null);
    }

    @Synchronized
    @Override
    public void addNewScope(String scope) {
        scopes.putIfAbsent(scope, new InMemoryScope());
    }

    @Synchronized
    @Override
    public void removeScope(String scope) {
        scopes.remove(scope);
    }

    @Synchronized
    @Override
    public Scope getScope(String scope) {
        return scopes.get(scope);
    }
}
