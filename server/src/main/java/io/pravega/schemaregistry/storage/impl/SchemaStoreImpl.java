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
import java.util.concurrent.CompletableFuture;

public class SchemaStoreImpl implements SchemaStore {
    private final Namespaces namespaces;

    public SchemaStoreImpl(Namespaces namespaces) {
        this.namespaces = namespaces;
    }
    
    // region schema store
    @Override
    public CompletableFuture<ListWithToken<String>> listNamespaces(ContinuationToken parse) {
        return namespaces.getNamespaces();
    }

    @Override
    public CompletableFuture<Void> createNamespace(String namespace) {
        return namespaces.addNewNamespace(namespace);
    }

    @Override
    public CompletableFuture<Void> deleteNamespace(String namespace) {
        return namespaces.removeNamespace(namespace);
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listGroups(String namespace, @Nullable ContinuationToken token) {
        return getNamespace(namespace).thenCompose(Namespace::getGroups);
    }

    @Override
    public CompletableFuture<Boolean> createGroupInNamespace(String namespace, String group, GroupProperties groupProperties) {
        return getNamespace(namespace).thenCompose(scp -> scp.addNewGroup(group, groupProperties));
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        return getNamespace(namespace).thenCompose(scp -> scp.deleteGroup(group));
    }

    @Override
    public CompletableFuture<Position> getGroupEtag(String namespace, String group) {
        return getGroup(namespace, group).thenCompose(Group::getCurrentEtag);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group) {
        return getGroup(namespace, group).thenCompose(grp ->
                grp.getCurrentValidationRules().thenApply(rules -> 
                new GroupProperties(grp.getSchemaType(), rules, grp.isSubgroupByEventType(), grp.isEnableEncoding())));
    }

    @Override
    public CompletableFuture<Position> updateValidationPolicy(String namespace, String group, Position etag, SchemaValidationRules policy) {
        return getGroup(namespace, group)
                .thenCompose(grp -> grp.getCurrentEtag().thenCompose(currentEtag -> {
                    if (!etag.equals(currentEtag)) {
                        throw new StoreExceptions.WriteConflictException();
                    } else {
                        return grp.updateValidationPolicy(policy, etag);
                    }
                }));
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listSubGroups(String namespace, String group, ContinuationToken token) {
        return getGroup(namespace, group).thenCompose(Group::getSubgroups);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInGroup(String namespace, String group, ContinuationToken token) {
        return getGroup(namespace, group).thenCompose(Group::getSchemas);
    }

    @Override
    public CompletableFuture<ListWithToken<SchemaWithVersion>> listSchemasInSubgroup(String namespace, String group, String subgroup, ContinuationToken token) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getSchemas(subgroup));
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String group, VersionInfo versionInfo) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getSchema(versionInfo));
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group) {
        return getGroup(namespace, group).thenCompose(Group::getLatestSchema);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group, String subgroup) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getLatestSchema(subgroup));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToGroup(String namespace, String group, Position etag, SchemaInfo schemaInfo) {
        return getGroup(namespace, group).thenCompose(grp -> grp.addSchemaToGroup(schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaToSubgroup(String namespace, String group, String subgroup, Position etag, SchemaInfo schemaInfo) {
        return getGroup(namespace, group).thenCompose(grp -> grp.addSchemaToSubGroup(subgroup, schemaInfo, etag));
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schemaInfo) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getVersion(schemaInfo));
    }

    @Override
    public CompletableFuture<EncodingId> createOrGetEncodingId(String namespace, String group, VersionInfo versionInfo,
                                                               CompressionType compressionType) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getOrCreateEncodingId(versionInfo, compressionType));
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getEncodingInfo(encodingId));
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String namespace, String group) {
        return getGroup(namespace, group).thenCompose(Group::getCompressions);
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getGroupHistory(String namespace, String group) {
        return getGroup(namespace, group).thenCompose(Group::getHistory);
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getSubGroupHistory(String namespace, String group, String subgroup) {
        return getGroup(namespace, group).thenCompose(grp -> grp.getHistory(subgroup));
    }
    // endregion

    private CompletableFuture<Group> getGroup(String namespace, String group) {
        return getNamespace(namespace)
                .thenCompose(scp -> scp.getGroup(group)
                                       .thenApply(grp -> {
                                           if (group == null) {
                                               throw new StoreExceptions.DataNotFoundException();
                                           }

                                           return grp;
                                       }));
    }

    private CompletableFuture<Namespace> getNamespace(String namespace) {
        return namespaces.getNamespace(namespace)
                         .thenApply(scp -> {
                             if (scp == null) {
                                 throw new StoreExceptions.DataNotFoundException();
                             }

                             return scp;
                         });
    }
}
