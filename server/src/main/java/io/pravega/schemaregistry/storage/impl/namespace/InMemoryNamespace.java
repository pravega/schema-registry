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
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.InMemoryIndex;
import io.pravega.schemaregistry.storage.impl.group.InMemoryLog;
import io.pravega.schemaregistry.storage.impl.group.Index;
import io.pravega.schemaregistry.storage.impl.group.Log;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class InMemoryNamespace implements Namespace<Integer> {
    @GuardedBy("$lock")
    private final Map<String, Group<Integer>> groups = new HashMap<>();
    private final Supplier<Log> walFactory;
    private final Supplier<Index<Integer>> kvFactory;
    private final ScheduledExecutorService executor;

    public InMemoryNamespace(ScheduledExecutorService executor) {
        this.executor = executor;
        this.walFactory = InMemoryLog::new;
        this.kvFactory = InMemoryIndex::new;
    }

    @Synchronized
    @Override
    public CompletableFuture<Group<Integer>> getGroup(String groupName) {
        return CompletableFuture.completedFuture(groups.get(groupName));
    }

    @Synchronized
    @Override
    public CompletableFuture<Boolean> addNewGroup(String group, GroupProperties groupProperties) {
        if (groups.containsKey(group)) {
            return CompletableFuture.completedFuture(false);
        }
        Group<Integer> grp = groups.computeIfAbsent(group, 
                x -> new Group<>(walFactory.get(), kvFactory.get(), executor));
        return grp.create(groupProperties.getSchemaType(), groupProperties.isSubgroupBySchemaName(),
                groupProperties.isEnableEncoding(), groupProperties.getSchemaValidationRules()).thenApply(v -> true);
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getGroups() {
        return CompletableFuture.completedFuture(new ListWithToken<>(new ArrayList<>(groups.keySet()), null));
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        groups.remove(group);
        return CompletableFuture.completedFuture(null);
    }
}
