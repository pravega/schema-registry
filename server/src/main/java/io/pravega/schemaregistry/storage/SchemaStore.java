/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (String namespace, the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public interface SchemaStore {
    CompletableFuture<ListWithToken<String>> listGroups(String namespace, @Nullable ContinuationToken token, int limit);

    CompletableFuture<Boolean> createGroup(String namespace, String groupId, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String namespace, String groupId);

    CompletableFuture<Etag> getGroupEtag(String namespace, String groupId);

    CompletableFuture<GroupProperties> getGroupProperties(String namespace, String groupId);
    
    CompletableFuture<Void> updateCompatibility(String namespace, String groupId, Etag etag, Compatibility policy);

    CompletableFuture<List<SchemaWithVersion>> getLatestSchemas(String namespace, String groupId);
    
    CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String groupId);

    CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String groupId, VersionInfo from);

    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String groupId, String type);

    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String groupId, String type, VersionInfo from);
    
    CompletableFuture<Void> deleteSchema(String namespace, String groupId, int schemaId, Etag etag);
    
    CompletableFuture<Void> deleteSchema(String namespace, String groupId, String schemaType, int version, Etag etag);
    
    CompletableFuture<SchemaInfo> getSchema(String namespace, String groupId, int schemaId);
    
    CompletableFuture<SchemaInfo> getSchema(String namespace, String groupId, String schemaType, int version);

    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String groupId);
    
    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String groupId, String type);
    
    CompletableFuture<VersionInfo> addSchema(String namespace, String groupId, SchemaInfo schemaInfo, GroupProperties prop, Etag etag);

    CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String groupId, SchemaInfo schemaInfo);

    CompletableFuture<Either<EncodingId, Etag>> getEncodingId(String namespace, String groupId, VersionInfo versionInfo, String codecType);
    
    CompletableFuture<EncodingId> createEncodingId(String namespace, String groupId, VersionInfo versionInfo, String codecType, Etag etag);

    CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String groupId, EncodingId encodingId);

    CompletableFuture<List<String>> getCodecTypes(String namespace, String groupId);

    CompletableFuture<Void> addCodecType(String namespace, String groupId, String codecType);

    CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String namespace, String groupId);
    
    CompletableFuture<List<GroupHistoryRecord>> getGroupHistoryForType(String namespace, String groupId, String type);
    
    CompletableFuture<List<String>> getGroupsUsing(String namespace, SchemaInfo schemaInfo);
}
