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

import io.pravega.common.concurrent.Futures;
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
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SchemaStoreImpl implements SchemaStore {
    private final Scopes scopes;

    public SchemaStoreImpl(Scopes scopes) {
        this.scopes = scopes;
    }
    
    // region schema store
    @Override
    public CompletableFuture<ListWithToken<String>> listScopes(ContinuationToken parse) {
        return CompletableFuture.completedFuture(scopes.getScopes());
    }

    @Override
    public CompletableFuture<Void> createScope(String scope) {
        scopes.addNewScope(scope);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteScope(String scope) {
        scopes.removeScope(scope);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(String scope, @Nullable ContinuationToken token) {
        return getScope(scope).thenApply(scp -> {
            List<String> list = scp.getGroups().entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
            return new ListWithToken<>(list, null);
        });
    }

    @Override
    public CompletableFuture<Boolean> createGroupInScope(String scope, String group, GroupProperties groupProperties) {
        return getScope(scope).thenApply(scp -> scp.addNewGroup(group, groupProperties));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String scope, String group) {
        return getScope(scope).thenAccept(scp -> scp.deleteGroup(group));
    }

    @Override
    public CompletableFuture<Position> getGroupEtag(String scope, String group) {
        return getGroup(scope, group).thenApply(Group::getCurrentEtag);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String scope, String group) {
        return getGroup(scope, group).thenApply(grp -> new GroupProperties(grp.getSchemaType(),
                grp.getCurrentValidationRules(), grp.isSubgroupByEventType(), grp.isEnableEncoding()));
    }

    @Override
    public CompletableFuture<Position> updateValidationPolicy(String scope, String group, Position etag, SchemaValidationRules policy) {
        return getGroup(scope, group).thenApply(grp -> {
            if (!etag.equals(grp.getCurrentEtag())) {
                throw new StoreExceptions.WriteConflictException();
            } else {
                return grp.updateValidationPolicy(policy, etag);
            }
        });
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listSubGroups(String scope, String group, ContinuationToken token) {
        return getGroup(scope, group).thenApply(Group::getSubgroups);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String scope, String group, ContinuationToken token) {
        return getGroup(scope, group).thenApply(Group::getSchemas);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String scope, String group, String subgroup, ContinuationToken token) {
        return getGroup(scope, group).thenApply(grp -> grp.getSchemas(subgroup));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo versionInfo) {
        return getGroup(scope, group).thenApply(grp -> grp.getSchema(versionInfo));
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group) {
        return getGroup(scope, group).thenApply(Group::getLatestSchema);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, String subgroup) {
        return getGroup(scope, group).thenApply(grp -> grp.getLatestSchema(subgroup));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToGroup(String scope, String group, Position etag, SchemaInfo schemaInfo) {
        return getGroup(scope, group).thenApply(grp -> grp.addSchemaToGroup(schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToSubgroup(String scope, String group, String subgroup, Position etag, SchemaInfo schemaInfo) {
        return getGroup(scope, group).thenApply(grp -> grp.addSchemaToSubGroup(subgroup, schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schemaInfo) {
        return getGroup(scope, group).thenApply(grp -> grp.getVersion(schemaInfo));
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String scope, String group, VersionInfo versionInfo,
                                                               CompressionType compressionType) {
        return getGroup(scope, group).thenApply(grp -> grp.getOrCreateEncodingId(versionInfo, compressionType));
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId) {
        return getGroup(scope, group).thenApply(grp -> grp.getEncodingInfo(encodingId));
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String scope, String group) {
        return getGroup(scope, group).thenApply(Group::getCompressions);
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getGroupHistory(String scope, String group) {
        return getGroup(scope, group).thenApply(Group::getHistory);
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getSubGroupHistory(String scope, String group, String subgroup) {
        return getGroup(scope, group).thenApply(grp -> grp.getHistory(subgroup));
    }
    // endregion

    private CompletableFuture<Group> getGroup(String scope, String group) {
        return getScope(scope).thenApply(scp -> {
            Group grp = scp.getGroup(group);
            if (group == null) {
                throw new StoreExceptions.DataNotFoundException();
            }

            return grp;
        });
    }

    private CompletableFuture<Scope> getScope(String scope) {
        Scope scp = scopes.getScope(scope);
        if (scp == null) {
            return Futures.failedFuture(new StoreExceptions.DataNotFoundException());
        }

        return CompletableFuture.completedFuture(scp);
    }
}
