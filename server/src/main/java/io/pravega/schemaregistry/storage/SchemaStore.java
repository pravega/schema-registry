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
import io.pravega.schemaregistry.contract.data.CodecType;
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

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public interface SchemaStore {
    CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token);

    CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties);

    CompletableFuture<Void> deleteGroup(String group);

    CompletableFuture<Position> getGroupEtag(String group);

    CompletableFuture<GroupProperties> getGroupProperties(String group);
    
    CompletableFuture<Void> updateValidationRules(String group, Position etag, SchemaValidationRules policy);

    CompletableFuture<ListWithToken<String>> listObjectTypes(String group, ContinuationToken token);
    
    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemas(String group, ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemas(String group, VersionInfo from, ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasByObjectType(String group, String objectTypeName,
                                                                                ContinuationToken token);

    CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasByObjectType(String group, String objectTypeName,
                                                                                VersionInfo from, ContinuationToken token);
    
    CompletableFuture<SchemaInfo> getSchema(String group, VersionInfo versionInfo);

    CompletableFuture<SchemaWithVersion> getLatestSchema(String group, boolean sync);
    
    CompletableFuture<SchemaWithVersion> getLatestSchema(String group, String objectTypeName, boolean sync);
    
    CompletableFuture<VersionInfo> getLatestVersion(String group, boolean sync);
    
    CompletableFuture<VersionInfo> getLatestVersion(String group, String objectTypeName, boolean sync);

    CompletableFuture<VersionInfo> addSchemaToGroup(String group, SchemaInfo schemaInfo, VersionInfo versionInfo, Position etag);

    CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo);

    CompletableFuture<Either<EncodingId, Position>> getEncodingId(String group, VersionInfo versionInfo, CodecType codecType);
    
    CompletableFuture<EncodingId> createEncodingId(String group, VersionInfo versionInfo, CodecType codecType, Position etag);

    CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId);

    CompletableFuture<List<CodecType>> getCodecTypes(String group);

    CompletableFuture<ListWithToken<SchemaEvolution>> getGroupHistory(String group);
    
    CompletableFuture<ListWithToken<SchemaEvolution>> getGroupHistoryForObjectType(String group, String objectTypeName);
}
