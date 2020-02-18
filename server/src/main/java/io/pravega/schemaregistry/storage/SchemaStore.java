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

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SchemaStore {
    CompletableFuture<ListWithToken<String>> listScopes(ContinuationToken parse);
    
    CompletableFuture<Void> createScope(String scope);
    
    CompletableFuture<Void> deleteScope(String scope);
    
    CompletableFuture<ListWithToken<String>> listGroups(String scope, @Nullable ContinuationToken token);

    CompletableFuture<Boolean> createGroupInScope(String scope, String group, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String scope, String group);

    CompletableFuture<Group> getGroup(String scope, String group);
    
    CompletableFuture<Subgroup> getSubgroup(String scope, String group, String subgroup);
    
    CompletableFuture<GroupProperties> getGroupProperties(String scope, String group);
    
    CompletableFuture<Group> updateCompatibilityPolicy(Group group, SchemaValidationRules policy);

    CompletableFuture<ListWithToken<String>> listSubGroups(String scope, String group, ContinuationToken token);
    
    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String scope, String group,
                                                                                           ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String scope, String group,
                                                                                           String subgroup, 
                                                                                           ContinuationToken token);
    
    CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo versionInfo);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, String subgroup);

    CompletableFuture<VersionInfo> conditionallyAddSchemaToGroup(Group group, SchemaInfo schemaInfo);

    CompletableFuture<VersionInfo> conditionallyAddSchemaToSubgroup(Subgroup subgroup, SchemaInfo schemaInfo);
    
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schemaInfo);
    
    CompletableFuture<EncodingId> createOrGetEncodingId(String scope, String group, VersionInfo versionInfo, CompressionType compressionType);
    
    CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId);

    CompletableFuture<List<CompressionType>> getCompressions(String scope, String group);
}
