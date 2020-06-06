/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.groups;

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.GroupTable;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * In memory groups implementation. 
 */
public class InMemoryGroups implements Groups<Integer> {
    @GuardedBy("$lock")
    private final Map<String, Group<Integer>> groups = new HashMap<>();
    private final Supplier<GroupTable<Integer>> kvFactory;
    private final ScheduledExecutorService executor;

    public InMemoryGroups(ScheduledExecutorService executor) {
        this.executor = executor;
        this.kvFactory = InMemoryGroupTable::new;
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
                x -> {
                    
                    return new Group<>(group, kvFactory.get(), executor);
                });
        return grp.create(groupProperties.getSerializationFormat(), groupProperties.getProperties(), groupProperties.isAllowMultipleTypes(), 
                groupProperties.getSchemaValidationRules()).thenApply(v -> true);
    }

    @Synchronized
    @Override
    public CompletableFuture<ListWithToken<String>> getGroups(ContinuationToken token, int limit) {
        ContinuationToken next = ContinuationToken.fromString(Integer.toString(groups.size()));
        if (token == null || token.equals(ContinuationToken.EMPTY)) {
            return CompletableFuture.completedFuture(new ListWithToken<>(new ArrayList<>(groups.keySet()), next));
        } else {
            return CompletableFuture.completedFuture(new ListWithToken<>(Collections.emptyList(), next));
        }
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        groups.remove(group);
        return CompletableFuture.completedFuture(null);
    }
}
