/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.Group;
import io.pravega.schemaregistry.storage.impl.groups.Groups;
import io.pravega.schemaregistry.storage.impl.schemas.Schemas;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
@Slf4j
public class SchemaStoreImpl<T> implements SchemaStore {
    private final Groups<T> groups;
    private final Schemas<T> schemas;

    public SchemaStoreImpl(Groups<T> groups, Schemas<T> schemas) {
        this.groups = groups;
        this.schemas = schemas;
    }

    // region schema store
    @Override
    public CompletableFuture<ResultPage<String, ContinuationToken>> listGroups(String namespace, @Nullable ContinuationToken token, int limit) {
        return groups.listGroups(namespace, token, limit);
    }

    @Override
    public CompletableFuture<Boolean> createGroup(String namespace, String groupId, GroupProperties groupProperties) {
        return groups.addNewGroup(namespace, groupId, groupProperties);
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String groupId) {
        return groups.deleteGroup(namespace, groupId);
    }

    @Override
    public CompletableFuture<Etag> getGroupEtag(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getCurrentEtag);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getGroupProperties);
    }

    @Override
    public CompletableFuture<Void> updateCompatibility(String namespace, String groupId, Etag etag, Compatibility policy) {
        return getGroup(namespace, groupId)
                .thenCompose(grp -> grp.updateValidationPolicy(policy, etag));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listLatestSchemas(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getLatestSchemas);
    }


    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getSchemas);
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String groupId, VersionInfo from) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getSchemas(from.getId()));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String groupId, String schemaType) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getSchemas(schemaType));
    }

    @Override
    public CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String groupId, String schemaType, VersionInfo from) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getSchemas(schemaType, from.getId()));
    }

    @Override
    public CompletableFuture<Void> deleteSchema(String namespace, String groupId, int schemaId, Etag etag) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.deleteSchema(schemaId, etag));
    }

    @Override
    public CompletableFuture<Void> deleteSchema(String namespace, String groupId, String schemaType, int version, String serializationFormat, Etag etag) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.deleteSchema(schemaType, version, serializationFormat, etag));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String groupId, int schemaId) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getSchema(schemaId));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String groupId, String schemaType, int version, String serializationFormat) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getSchema(schemaType, version, serializationFormat));
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getLatestSchemaVersion);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String groupId, String type) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getLatestSchemaVersion(type));
    }
    
    @Override
    public CompletableFuture<VersionInfo> addSchema(String namespace, String groupId, SchemaInfo schemaInfo, SchemaInfo normalized,
                                                    BigInteger fingerprint, GroupProperties prop, Etag etag) {
        // Store normalized form of schema with the global schemas while the original form is stored within the group.
        log.info("Add Schema called with params namespace = {}, groupId = {}, schemaInfo = {}, normalized = {}," +
                "etag = {}", namespace, groupId, schemaInfo, normalized, etag);
        return schemas.addSchema(normalized, namespace, groupId)
                .thenCompose(v -> getGroup(namespace, groupId).thenCompose(grp -> grp.addSchema(schemaInfo, fingerprint, prop, etag)));
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String groupId, SchemaInfo schemaInfo, BigInteger fingerprint) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getVersion(schemaInfo, fingerprint));
    }

    @Override
    public CompletableFuture<Either<EncodingId, Etag>> getEncodingId(String namespace, String groupId, VersionInfo versionInfo, String codecType) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getEncodingId(versionInfo, codecType));
    }

    @Override
    public CompletableFuture<EncodingId> createEncodingId(String namespace, String groupId, VersionInfo versionInfo, String codecType, 
                                                          Etag etag) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.createEncodingId(versionInfo, codecType, etag));
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String groupId, EncodingId encodingId) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getEncodingInfo(encodingId));
    }

    @Override
    public CompletableFuture<List<CodecType>> listCodecTypes(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getCodecTypes);
    }

    @Override
    public CompletableFuture<Void> addCodecType(String namespace, String groupId, CodecType codecType) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.addCodecType(codecType));
    }

    @Override
    public CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String namespace, String groupId) {
        return getGroup(namespace, groupId).thenCompose(Group::getHistory);
    }

    @Override
    public CompletableFuture<List<GroupHistoryRecord>> getGroupHistoryForType(String namespace, String groupId, String type) {
        return getGroup(namespace, groupId).thenCompose(grp -> grp.getHistory(type));
    }

    @Override
    public CompletableFuture<List<String>> getGroupsUsing(String namespace, SchemaInfo schemaInfo) {
        return schemas.getGroupsUsing(namespace, schemaInfo);
    }

    // endregion

    private CompletableFuture<Group<T>> getGroup(String namespace, String groupId) {
        log.info("GetGroup for namespace {} and groupId {}", namespace, groupId);
        return groups.getGroup(namespace, groupId).thenApply(grp -> {
                                           if (grp == null) {
                                               log.info("Get group is null");
                                               String errorMessage = String.format("group=%s/%s", namespace, groupId);
                                               throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, errorMessage);
                                           }

                                           return grp;
                                       });
    }
}
