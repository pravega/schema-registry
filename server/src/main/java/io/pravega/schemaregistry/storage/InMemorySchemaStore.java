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
import io.pravega.schemaregistry.contract.data.SchemaEvolutionEpoch;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InMemorySchemaStore implements SchemaStore {
    @Override
    public CompletableFuture<ListWithToken<String>> listScopes(ContinuationToken parse) {
        return null;
    }

    @Override
    public CompletableFuture<Void> createScope(String scope) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteScope(String scope) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(String scope, @Nullable ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> createGroupInScope(String scope, String group, GroupProperties groupProperties) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<Etag> getGroupEtag(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<Etag> updateCompatibilityPolicy(String scope, String group, Etag etag, SchemaValidationRules policy) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listSubGroups(String scope, String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String scope, String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String scope, String group, String subgroup, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo versionInfo) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, String subgroup) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> conditionallyAddSchemaToGroup(String scope, String group, Etag etag, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> conditionallyAddSchemaToSubgroup(String scope, String group, String subgroup, Etag etag, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String scope, String group, VersionInfo versionInfo, CompressionType compressionType) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId) {
        return null;
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getGroupHistory(String scope, String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getSubGroupHistory(String scope, String group, String subgroup) {
        return null;
    }
}
