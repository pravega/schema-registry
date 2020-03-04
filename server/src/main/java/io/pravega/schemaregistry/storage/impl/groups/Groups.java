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
import io.pravega.schemaregistry.storage.impl.group.Group;

import java.util.concurrent.CompletableFuture;

public interface Groups<T> {
    CompletableFuture<Group<T>> getGroup(String groupName);

    CompletableFuture<Boolean> addNewGroup(String group, GroupProperties groupProperties);

    CompletableFuture<ListWithToken<String>> getGroups();

    CompletableFuture<Void> deleteGroup(String group);
}
