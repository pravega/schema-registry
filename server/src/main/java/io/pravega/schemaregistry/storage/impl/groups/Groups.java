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

import java.util.concurrent.CompletableFuture;

/**
 * Groups table for doing operations on groups resource. 
 * 
 * @param <T> Type of Version for group. 
 */
public interface Groups<T> {
    /**
     * Get a handle to the group identified by namespace and group. Only groups that have been successfully created are 
     * returned by this method. 
     * If the group doesnt exist, OR is being created or deleted concurrently as this api is called, it will throw DataNotFoundException. 
     * 
     * @param namespace namespace 
     * @param group group
     * @return CompletableFuture which holds the handle to the group object. 
     */
    CompletableFuture<Group<T>> getGroup(String namespace, String group);

    /**
     * Add a new group to the groups table. this implementation is idempotent. If a group has already been added previously,
     * this api will return false, true otherwise. Adding a group is non atomic action as it performs updates across
     * two tables internally - add a new entry to groups table and create a group specific table and metadata within in. 
     * If concurrent attempts to add a group by the same name are attempted, only one of them will succeed while the other will
     * fail with Write Conflict. 
     * A group could be left in partially completed state if the service instance processing the create request crashes before
     * it could complete the group creation. 
     * Such groups are not listed as part of list groups API. Any subsequent attempt to create the group by the same name
     * will complete the previous attempt and if new create request's parameters {@link GroupProperties} matche the group that is created, then
     * the api would return true, false otherwise. 
     * 
     * @param namespace namespace
     * @param group group
     * @param groupProperties group properties. 
     * @return Completable future which when completed will indicate whether this was a new group or an idempotent create 
     * is performed. 
     */
    CompletableFuture<Boolean> addNewGroup(String namespace, String group, GroupProperties groupProperties);

    /**
     * A paginated list request that would return a list of groups from the supplied continuation token till it has fetched
     * "limit" number of groups. The implementation of this API is not guaranteed to be atomic and if groups are added
     * while this api is retrieving the results, the newly added groups may or may not get included as part of the response. 
     * The response also includes the continuation token pointint to the last returned element. 
     * So any group that was concurrently created as this API returned the responses can be accessed by requesting list groups
     * from the returned continuation token. 
     * 
     * @param namespace namespace.
     * @param token continuation token.
     * @param limit number of groups to return. 
     * @return List of groups with continuation token. 
     */
    CompletableFuture<ListWithToken<String>> listGroups(String namespace, ContinuationToken token, int limit);

    /**
     * Deletes a group. This api is idempotent and invoking it multiple times is safe. A group can only be deleted
     * after it has been successfully created and is visible to the user as part of list groups api. Delete group implementation 
     * should not attempt to delete a group until it has been created completely. This ensures that it will not race against
     * a concurrent create request. 
     * 
     * @param namespace namespace.
     * @param group group. 
     * @return CompletableFuture which when completed will indicate that a group has been deleted successfully. 
     */
    CompletableFuture<Void> deleteGroup(String namespace, String group);
}
