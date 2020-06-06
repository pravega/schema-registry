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
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.groups.Groups;
import io.pravega.schemaregistry.storage.impl.schemas.Schemas;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SchemaStoreImpl<T> implements SchemaStore {
    private final Groups<T> groups;
    private final Schemas<T> schemas;

    public SchemaStoreImpl(Groups<T> groups, Schemas<T> schemas) {
        this.groups = groups;
        this.schemas = schemas;
    }
    
    // region schema store
    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(@Nullable ContinuationToken token, int limit) {
        return groups.getGroups(token, limit);
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
    public CompletableFuture<Etag> getGroupEtag(String group) {
        return getGroup(group).thenCompose(Group::getCurrentEtag);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String group) {
        return getGroup(group).thenCompose(Group::getGroupProperties);
    }

    @Override
    public CompletableFuture<Void> updateValidationRules(String group, Etag etag, SchemaValidationRules policy) {
        return getGroup(group)
                .thenCompose(grp -> grp.updateValidationPolicy(policy, etag));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> getLatestSchemas(String group) {
        return getGroup(group).thenCompose(Group::getLatestSchemas);
    }


    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemas(String group) {
        return getGroup(group).thenCompose(Group::getSchemas);
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemas(String group, VersionInfo from) {
        return getGroup(group).thenCompose(grp -> grp.getSchemas(from.getOrdinal()));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String group, String type) {
        return getGroup(group).thenCompose(grp -> grp.getSchemas(type));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String group, String type, VersionInfo from) {
        return getGroup(group).thenCompose(grp -> grp.getSchemas(type, from.getOrdinal()));
    }

    @Override
    public CompletableFuture<Void> deleteSchema(String group, int versionOrdinal, Etag etag) {
        return getGroup(group).thenCompose(grp -> grp.deleteSchema(versionOrdinal, etag));
    }

    @Override
    public CompletableFuture<Void> deleteSchema(String group, String schemaType, int version, Etag etag) {
        return getGroup(group).thenCompose(grp -> grp.deleteSchema(schemaType, version, etag));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String group, int versionOrdinal) {
        return getGroup(group).thenCompose(grp -> grp.getSchema(versionOrdinal));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String group, String schemaType, int version) {
        return getGroup(group).thenCompose(grp -> grp.getSchema(schemaType, version));
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String group) {
        return getGroup(group).thenCompose(Group::getLatestSchemaVersion);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String group, String type) {
        return getGroup(group).thenCompose(grp -> grp.getLatestSchemaVersion(type));
    }
    
    @Override
    public CompletableFuture<VersionInfo> addSchema(String group, SchemaInfo schemaInfo, GroupProperties prop, Etag etag) {
        return schemas.addNewSchema(schemaInfo, group)
                .thenCompose(v -> getGroup(group).thenCompose(grp -> grp.addSchema(schemaInfo, prop, etag)));
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schemaInfo) {
        return getGroup(group).thenCompose(grp -> grp.getVersion(schemaInfo));
    }

    @Override
    public CompletableFuture<Either<EncodingId, Etag>> getEncodingId(String group, VersionInfo versionInfo, String codecType) {
        return getGroup(group).thenCompose(grp -> grp.getEncodingId(versionInfo, codecType));
    }

    @Override
    public CompletableFuture<EncodingId> createEncodingId(String group, VersionInfo versionInfo, String codecType, 
                                                          Etag etag) {
        return getGroup(group).thenCompose(grp -> grp.createEncodingId(versionInfo, codecType, etag));
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId) {
        return getGroup(group).thenCompose(grp -> grp.getEncodingInfo(encodingId));
    }

    @Override
    public CompletableFuture<List<String>> getCodecTypes(String group) {
        return getGroup(group).thenCompose(Group::getCodecTypes);
    }

    @Override
    public CompletableFuture<Void> addCodecType(String group, String codecType) {
        return getGroup(group).thenCompose(grp -> grp.addCodecType(codecType));
    }

    @Override
    public CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String group) {
        return getGroup(group).thenCompose(Group::getHistory);
    }

    @Override
    public CompletableFuture<List<GroupHistoryRecord>> getGroupHistoryForType(String group, String type) {
        return getGroup(group).thenCompose(grp -> grp.getHistory(type));
    }

    @Override
    public CompletableFuture<List<String>> getGroupsUsing(SchemaInfo schemaInfo) {
        return schemas.getGroupsUsing(schemaInfo);
    }

    // endregion

    private CompletableFuture<Group<T>> getGroup(String group) {
        return groups.getGroup(group).thenApply(grp -> {
                                           if (grp == null) {
                                               String errorMessage = String.format("group=%s", group);
                                               throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, errorMessage);
                                           }

                                           return grp;
                                       });
    }
}
