/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

public class InMemorySchemaStore implements SchemaStore {
    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        return null;
    }

    @Override
    public CompletableFuture<Position> getGroupEtag(String group) {
        return null;
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String group) {
        return null;
    }

    @Override
    public CompletableFuture<Position> updateValidationPolicy(String group, Position position, SchemaValidationRules policy) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listEventTypes(String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemas(String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasByEventType(String group, String eventTypeName, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String group, VersionInfo versionInfo) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group, String eventTypeName) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToGroup(String group, Position position, SchemaInfo schemaInfo) {
        return null;
    }
    
    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String group, VersionInfo versionInfo, CompressionType compressionType) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId) {
        return null;
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolution>> getGroupHistory(String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolution>> getGroupHistoryForEventType(String group, String eventTypeName) {
        return null;
    }
}
