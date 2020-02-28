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
    CompletableFuture<ListWithToken<String>> listNamespaces(ContinuationToken parse);
    
    CompletableFuture<Void> createNamespace(String namespace);
    
    CompletableFuture<Void> deleteNamespace(String namespace);
    
    CompletableFuture<ListWithToken<String>> listGroups(String namespace, @Nullable ContinuationToken token);

    CompletableFuture<Boolean> createGroupInNamespace(String namespace, String group, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String namespace, String group);

    CompletableFuture<Position> getGroupEtag(String namespace, String group);
    
    CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group);
    
    CompletableFuture<Position> updateValidationPolicy(String namespace, String group, Position etag, SchemaValidationRules policy);

    CompletableFuture<ListWithToken<String>> listSubGroups(String namespace, String group, ContinuationToken token);
    
    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String namespace, String group,
                                                                                           ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String namespace, String group,
                                                                                           String subgroup, 
                                                                                           ContinuationToken token);
    
    CompletableFuture<SchemaInfo> getSchema(String namespace, String group, VersionInfo versionInfo);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group, String subgroup);

    CompletableFuture<VersionInfo> addSchemaToGroup(String namespace, String group, Position etag, SchemaInfo schemaInfo);

    CompletableFuture<VersionInfo> addSchemaToSubgroup(String namespace, String group, String subgroup, Position etag, SchemaInfo schemaInfo);
    
    CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schemaInfo);
    
    CompletableFuture<EncodingId> createOrGetEncodingId(String namespace, String group, VersionInfo versionInfo, CompressionType compressionType);
    
    CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId);

    CompletableFuture<List<CompressionType>> getCompressions(String namespace, String group);

    CompletableFuture<List<SchemaEvolution>> getGroupHistory(String namespace, String group);
    
    CompletableFuture<List<SchemaEvolution>> getSubGroupHistory(String namespace, String group, String subgroup);
}
