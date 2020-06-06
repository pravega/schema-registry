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
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public interface SchemaStore {
    CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token, int limit);

    CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String group);

    CompletableFuture<Etag> getGroupEtag(String group);

    CompletableFuture<GroupProperties> getGroupProperties(String group);
    
    CompletableFuture<Void> updateValidationRules(String group, Etag etag, SchemaValidationRules policy);

    CompletableFuture<List<SchemaWithVersion>> getLatestSchemas(String group);
    
    CompletableFuture<List<SchemaWithVersion>> listSchemas(String group);

    CompletableFuture<List<SchemaWithVersion>> listSchemas(String group, VersionInfo from);

    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String group, String type);

    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String group, String type, VersionInfo from);
    
    CompletableFuture<Void> deleteSchema(String group, int versionOrdinal, Etag etag);
    
    CompletableFuture<Void> deleteSchema(String group, String schemaType, int version, Etag etag);
    
    CompletableFuture<SchemaInfo> getSchema(String group, int versionOrdinal);
    
    CompletableFuture<SchemaInfo> getSchema(String group, String schemaType, int version);

    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String group);
    
    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String group, String type);
    
    CompletableFuture<VersionInfo> addSchema(String group, SchemaInfo schemaInfo, GroupProperties prop, Etag etag);

    CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo);

    CompletableFuture<Either<EncodingId, Etag>> getEncodingId(String group, VersionInfo versionInfo, String codecType);
    
    CompletableFuture<EncodingId> createEncodingId(String group, VersionInfo versionInfo, String codecType, Etag etag);

    CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId);

    CompletableFuture<List<String>> getCodecTypes(String group);

    CompletableFuture<Void> addCodecType(String group, String codecType);

    CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String group);
    
    CompletableFuture<List<GroupHistoryRecord>> getGroupHistoryForType(String group, String type);
    
    CompletableFuture<List<String>> getGroupsUsing(SchemaInfo schemaInfo);
}
