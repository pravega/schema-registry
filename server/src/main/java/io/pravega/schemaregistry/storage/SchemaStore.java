/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.schemaregistry.contract.Compatibility;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.*;

public interface SchemaStore {
    CompletableFuture<Void> createScope(String scope);
    
    CompletableFuture<Pair<List<Group>, ContinuationToken>> listGroups(String scope, String group, GroupProperties groupProperties,
                                                                       @Nullable ContinuationToken token);

    CompletableFuture<Group> createGroupInScope(String scope, String group, GroupProperties groupProperties);
    
    CompletableFuture<Group> getGroup(String scope, String group);
    
    CompletableFuture<Subgroup> getSubgroup(String scope, String group, String subgroup);
    
    CompletableFuture<GroupProperties> getGroupProperties(String scope, String group);
    
    CompletableFuture<Group> updateCompatibilityPolicy(Group group, Compatibility policy);

    CompletableFuture<List<String>> listSubGroups(String scope, String group);
    
    CompletableFuture<Pair<List<SchemaWithVersion>, ContinuationToken>> listSchemasInGroup(String scope, String group, 
                                                                                           ContinuationToken token);

    CompletableFuture<Pair<List<SchemaWithVersion>, ContinuationToken>> listSchemasInSubgroup(String scope, String group,
                                                                                           String subgroup, 
                                                                                           ContinuationToken token);
    
    CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo versionInfo);

    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group);

    CompletableFuture<SchemaInfo> getSchema(String scope, String group, String subgroup, VersionInfo versionInfo);

    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, String subgroup);

    CompletableFuture<VersionInfo> conditionallyAddSchemaToGroup(Group group, SchemaInfo schemaInfo);

    CompletableFuture<VersionInfo> conditionallyAddSchemaToSubgroup(Group group, Subgroup subgroup, SchemaInfo schemaInfo);
    
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schemaInfo);
    
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, String subgroup, SchemaInfo schemaInfo);

    CompletableFuture<EncodingId> createOrGetEncodingId(Group group, EncodingInfo encodingInfo);
    
    CompletableFuture<EncodingInfo> getEncodingInfo(Group group, EncodingId encodingId);

    
}
