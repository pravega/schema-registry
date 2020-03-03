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
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SchemaStore {
    CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token);

    CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String group);

    CompletableFuture<Position> getGroupEtag(String group);
    
    CompletableFuture<GroupProperties> getGroupProperties(String group);
    
    CompletableFuture<Position> updateValidationPolicy(String group, Position etag, SchemaValidationRules policy);

    CompletableFuture<ListWithToken<String>> listSubGroups(String group, ContinuationToken token);
    
    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String group,
                                                                                           ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String group,
                                                                                           String subgroup, 
                                                                                           ContinuationToken token);
    
    CompletableFuture<SchemaInfo> getSchema(String group, VersionInfo versionInfo);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String group);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String group, String subgroup);

    CompletableFuture<VersionInfo> addSchemaToGroup(String group, Position etag, SchemaInfo schemaInfo);

    CompletableFuture<VersionInfo> addSchemaToSubgroup(String group, String subgroup, Position etag, SchemaInfo schemaInfo);
    
    CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo);
    
    CompletableFuture<EncodingId> createOrGetEncodingId(String group, VersionInfo versionInfo, CompressionType compressionType);
    
    CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId);

    CompletableFuture<List<CompressionType>> getCompressions(String group);

    CompletableFuture<List<SchemaEvolution>> getGroupHistory(String group);
    
    CompletableFuture<List<SchemaEvolution>> getSubGroupHistory(String group, String subgroup);
}
