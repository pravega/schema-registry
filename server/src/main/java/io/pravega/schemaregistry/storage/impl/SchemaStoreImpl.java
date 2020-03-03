/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

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
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.groups.Groups;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SchemaStoreImpl<T> implements SchemaStore {
    private final Groups<T> groups;

    public SchemaStoreImpl(Groups<T> groups) {
        this.groups = groups;
    }
    
    // region schema store
    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token) {
        return groups.getGroups();
    }

    @Override
    public CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties) {
        return groups.addNewGroup(group, groupProperties);
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String group) {
        return groups.deleteGroup(group);
    }

    @Override
    public CompletableFuture<Position> getGroupEtag(String group) {
        return getGroup(group).thenCompose(Group::getCurrentEtag);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String group) {
        return getGroup(group).thenCompose(Group::getGroupProperties);
    }

    @Override
    public CompletableFuture<Void> updateValidationPolicy(String group, Position etag, SchemaValidationRules policy) {
        return getGroup(group)
                .thenCompose(grp -> grp.getCurrentEtag().thenCompose(currentEtag -> {
                    if (!etag.equals(currentEtag)) {
                        throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, "Validation Policy Update");
                    } else {
                        return grp.updateValidationPolicy(policy, etag);
                    }
                }));
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listSubGroups(String group) {
        return getGroup(group).thenCompose(Group::getSubgroups);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String group) {
        return getGroup(group).thenCompose(Group::getSchemas);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String group, VersionInfo from) {
        return getGroup(group).thenCompose(grp -> grp.getSchemas(from));
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String group, String subgroup) {
        return getGroup(group).thenCompose(grp -> grp.getSchemas(subgroup));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String group, VersionInfo versionInfo) {
        return getGroup(group).thenCompose(grp -> grp.getSchema(versionInfo));
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group) {
        return getGroup(group).thenCompose(Group::getLatestSchema);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group, String subgroup) {
        return getGroup(group).thenCompose(grp -> grp.getLatestSchema(subgroup));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToGroup(String group, Position etag, SchemaInfo schemaInfo) {
        return getGroup(group).thenCompose(grp -> grp.addSchemaToGroup(schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToSubgroup(String group, String subgroup, Position etag, SchemaInfo schemaInfo) {
        return getGroup(group).thenCompose(grp -> grp.addSchemaToSubGroup(subgroup, schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo) {
        return getGroup(group).thenCompose(grp -> grp.getVersion(schemaInfo));
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String group, VersionInfo versionInfo,
                                                               CompressionType compressionType) {
        return getGroup(group).thenCompose(grp -> grp.getOrCreateEncodingId(versionInfo, compressionType));
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId) {
        return getGroup(group).thenCompose(grp -> grp.getEncodingInfo(encodingId));
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String group) {
        return getGroup(group).thenCompose(Group::getCompressions);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaEvolution>> getGroupHistory(String group) {
        return getGroup(group).thenCompose(Group::getHistory);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaEvolution>> getSubGroupHistory(String group, String subgroup) {
        return getGroup(group).thenCompose(grp -> grp.getHistory(subgroup));
    }
    // endregion

    private CompletableFuture<Group<T>> getGroup(String group) {
        return groups.getGroup(group).thenApply(grp -> {
                                           if (grp == null) {
                                               String errorMessage = String.format("groups=%s, group=%s", group);
                                               throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, errorMessage);
                                           }

                                           return grp;
                                       });
    }
}
