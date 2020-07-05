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

import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.group.InMemoryGroupTable;
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * In memory groups implementation. 
 */
public class InMemoryGroups implements Groups<Integer> {
    @GuardedBy("$lock")
    private final Map<NamespaceAndGroup, Group<Integer>> groups = new HashMap<>();
    private final Supplier<InMemoryGroupTable> kvFactory;
    private final ScheduledExecutorService executor;

    public InMemoryGroups(ScheduledExecutorService executor) {
        this.executor = executor;
        this.kvFactory = InMemoryGroupTable::new;
    }

    @Synchronized
    @Override
    public CompletableFuture<Group<Integer>> getGroup(String namespace, String group) {
        return CompletableFuture.completedFuture(groups.get(new NamespaceAndGroup(namespace, group)));
    }

    @Synchronized
    @Override
    public CompletableFuture<Boolean> addNewGroup(String namespace, String group, GroupProperties groupProperties) {
        if (groups.containsKey(new NamespaceAndGroup(namespace, group))) {
            return CompletableFuture.completedFuture(false);
        }
        Group<Integer> grp = groups.computeIfAbsent(new NamespaceAndGroup(namespace, group), 
                x -> new Group<>(kvFactory.get(), executor));
        return grp.create(groupProperties.getSerializationFormat(), groupProperties.getProperties(), groupProperties.isAllowMultipleTypes(), 
                groupProperties.getCompatibility()).thenApply(v -> true);
    }

    @Synchronized
    @Override
    public CompletableFuture<ResultPage<String>> listGroups(String namespace, ContinuationToken token, int limit) {
        // TODO: pagination -- return only limit number of records!!
        String nameSpace = namespace == null ? "" : namespace;
        ContinuationToken next = ContinuationToken.fromString(Integer.toString(groups.size()));
        if (token == null || token.equals(ContinuationToken.EMPTY)) {
            List<String> namespaceAndGroups = groups.keySet().stream()
                                                    .filter(x -> x.getNamespace().equals(nameSpace))
                                                    .map(NamespaceAndGroup::getGroupId)
                                                    .collect(Collectors.toList());
            ResultPage<String> namespaceAndGroupResultPage = new ResultPage<>(namespaceAndGroups, next);
            return CompletableFuture.completedFuture(namespaceAndGroupResultPage);
        } else {
            return CompletableFuture.completedFuture(new ResultPage<>(Collections.emptyList(), next));
        }
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        groups.remove(new NamespaceAndGroup(namespace, group));
        return CompletableFuture.completedFuture(null);
    }
}
