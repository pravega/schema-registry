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

import io.pravega.schemaregistry.contract.data.GroupProperties;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class InMemoryScope implements Scope {
    @GuardedBy("$lock")
    private final Map<String, Group> groups = new HashMap<>();
    private final Supplier<Log> walFactory;
    private final Supplier<KeyValue> kvFactory;

    public InMemoryScope() {
        this.walFactory = InMemoryLog::new;
        this.kvFactory = InMemoryKeyValue::new;
    }

    @Synchronized
    @Override
    public Group getGroup(String groupName) {
        return groups.get(groupName);
    }

    @Synchronized
    @Override
    public boolean addNewGroup(String group, GroupProperties groupProperties) {
        if (groups.containsKey(group)) {
            return false;
        }
        groups.computeIfAbsent(group, x -> {
            return new Group(groupProperties.getSchemaType(), groupProperties.isSubgroupBySchemaName(),
                    groupProperties.isEnableEncoding(), groupProperties.getSchemaValidationRules(), walFactory.get(), kvFactory.get());
        });
        return true;
    }

    @Synchronized
    @Override
    public Map<String, Group> getGroups() {
        return Collections.unmodifiableMap(groups);
    }

    @Synchronized
    @Override
    public void deleteGroup(String group) {
        groups.remove(group);
    }
}
