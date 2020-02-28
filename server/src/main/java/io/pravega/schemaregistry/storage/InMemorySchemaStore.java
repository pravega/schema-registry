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
    public CompletableFuture<ListWithToken<String>> listNamespaces(ContinuationToken parse) {
        return null;
    }

    @Override
    public CompletableFuture<Void> createNamespace(String namespace) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteNamespace(String namespace) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(String namespace, @Nullable ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> createGroupInNamespace(String namespace, String group, GroupProperties groupProperties) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<Etag> getGroupEtag(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<Etag> updateCompatibilityPolicy(String namespace, String group, Etag etag, SchemaValidationRules policy) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listSubGroups(String namespace, String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String namespace, String group, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String namespace, String group, String subgroup, ContinuationToken token) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String group, VersionInfo versionInfo) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group, String subgroup) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> conditionallyAddSchemaToGroup(String namespace, String group, Etag etag, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> conditionallyAddSchemaToSubgroup(String namespace, String group, String subgroup, Etag etag, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schemaInfo) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String namespace, String group, VersionInfo versionInfo, CompressionType compressionType) {
        return null;
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId) {
        return null;
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolution>> getGroupHistory(String namespace, String group) {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaEvolution>> getSubGroupHistory(String namespace, String group, String subgroup) {
        return null;
    }
}
