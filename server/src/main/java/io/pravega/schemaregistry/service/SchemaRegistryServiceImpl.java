/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolutionEpoch;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.rules.CompatibilityChecker;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SchemaRegistryServiceImpl implements SchemaRegistryService {
    private final SchemaStore store;

    public SchemaRegistryServiceImpl(SchemaStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<ListWithToken<String>> listScopes(String continuationToken) {
        return store.listScopes(ContinuationToken.parse(continuationToken));
    }

    @Override
    public CompletableFuture<Void> createScope(String scope) {
        return store.createScope(scope);
    }

    @Override
    public CompletableFuture<Void> deleteScope(String scope) {
        return store.deleteScope(scope);
    }

    @Override
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroupsInScope(String scope, String continuationToken) {
        return store.listGroups(scope, ContinuationToken.parse(continuationToken))
                .thenCompose(reply -> {
                    ContinuationToken token = reply.getToken();
                    List<String> list = reply.getList();
                    return Futures.allOfWithResults(list.stream().collect(Collectors.toMap(x -> x, x -> store.getGroupProperties(scope, x))))
                           .thenApply(groups -> new MapWithToken<>(groups, token));
                });
    }

    @Override
    public CompletableFuture<Boolean> createGroup(String scope, String group, GroupProperties groupProperties) {
        return store.createGroupInScope(scope, group, groupProperties);
    }

    @Override
    public CompletableFuture<GroupProperties> getGroupProperties(String scope, String group) {
        return store.getGroupProperties(scope, group);
    }

    @Override
    public CompletableFuture<Void> updateSchemaValidationPolicy(String scope, String group, SchemaValidationRules validationRules) {
        return Futures.toVoid(store.getGroup(scope, group)
                .thenCompose(g -> store.updateCompatibilityPolicy(g, validationRules)));
    }

    @Override
    public CompletableFuture<ListWithToken<String>> getSubgroups(String scope, String group, ContinuationToken token) {
        return store.listSubGroups(scope, group, token);
    }

    @Override
    public CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String group, SchemaInfo schema, SchemaValidationRules rules) {
        // 1. get group policy
        // 2. get checker for schema type.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        CompatibilityChecker checker;
        
        return store.getGroupProperties(scope, group)
                         .thenCompose(prop -> {
                             if (prop.isSubgroupBySchemaName()) {
                                 String subgroup = schema.getName();
                                 return store.getSubgroup(scope, group, subgroup)
                                             // todo: apply policy
                                             .thenCompose(subgrp -> store.conditionallyAddSchemaToSubgroup(subgrp, schema));
                             } else {
                                 return store.getGroup(scope, group)
                                             // todo: apply policy
                                             .thenCompose(grp -> store.conditionallyAddSchemaToGroup(grp, schema));
                             }
                         });
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo version) {
        return store.getSchema(scope, group, version);
    }

    @Override
    public CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId) {
        return store.getEncodingInfo(scope, group, encodingId);
    }

    @Override
    public CompletableFuture<EncodingId> getEncodingId(String scope, String group, VersionInfo version, CompressionType compressionType) {
        return store.createOrGetEncodingId(scope, group, version, compressionType);
    }

    @Override
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, @Nullable String subgroup) {
        if (subgroup == null) {
            return store.getLatestSchema(scope, group);
        } else {
            return store.getLatestSchema(scope, group, subgroup);
        }
    }

    @Override
    public CompletableFuture<List<SchemaEvolutionEpoch>> getGroupEvolutionHistory(String scope, String group, @Nullable String subgroup) {
        return null;
    }

    @Override
    public CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schema) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> validateSchema(String scope, String group, SchemaInfo schema, SchemaValidationRules validationRules) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteGroup(String scope, String group) {
        return store.deleteGroup(scope, group);
    }

    @Override
    public CompletableFuture<List<CompressionType>> getCompressions(String scope, String group) {
        return store.getCompressions(scope, group);
    }
}
