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
import java.util.InputMismatchException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SchemaRegistryService {
    private final SchemaStore store;

    public SchemaRegistryService(SchemaStore store) {
        this.store = store;
    }

    /**
     * Lists all scopes.
     *
     * @param continuationToken Continuation Token. 
     * @return CompletableFuture which holds list of scope names upon completion. 
     */
    public CompletableFuture<ListWithToken<String>> listNamespaces(String continuationToken) {
        return store.listNamespaces(ContinuationToken.parse(continuationToken));
    }

    /**
     * Creates new scope if absent. This api is idempotent. 
     * @param scope Name of scope.
     * @return CompletableFuture which is completed when create scope completes.
     */
    public CompletableFuture<Void> createNamespace(String scope) {
        return store.createNamespace(scope);
    }

    /**
     * Deletes a scope.  
     * @param scope Name of scope.
     * @return CompletableFuture which is completed when delete scope completes.
     */
    public CompletableFuture<Void> deleteNamespace(String scope) {
        return store.deleteNamespace(scope);
    }

    /**
     * Lists groups in scope. 
     *
     * @param scope Name of scope.
     * @param continuationToken continuation token
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroupsInNamespace(String scope, String continuationToken) {
        return store.listGroups(scope, ContinuationToken.parse(continuationToken))
                .thenCompose(reply -> {
                    ContinuationToken token = reply.getToken();
                    List<String> list = reply.getList();
                    return Futures.allOfWithResults(list.stream().collect(Collectors.toMap(x -> x, x -> store.getGroupProperties(scope, x))))
                           .thenApply(groups -> new MapWithToken<>(groups, token));
                });
    }

    /**
     * Creates new group within a scope. Idempotent behaviour. If group already exists, it returns false. 
     *
     * @param scope Name of scope.
     * @param group Name of group. 
     * @param groupProperties Group properties.
     * @return CompletableFuture which is completed when create group completes. True indicates this was 
     * new group, false indicates it was an existing group. 
     */
    public CompletableFuture<Boolean> createGroup(String scope, String group, GroupProperties groupProperties) {
        return store.createGroupInNamespace(scope, group, groupProperties);
    }

    /**
     * Gets group's properties. 
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#subgroupBySchemaName} that specifies if schemas are subgrouped by event type. 
     * Event Types are uniquely identified by {@link SchemaInfo#name}. 
     * {@link GroupProperties#enableEncoding} describes whether registry should generate encoding ids to identify 
     * encoding properties in {@link EncodingInfo}.
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @return CompletableFuture which holds group properties upon completion. 
     */
    public CompletableFuture<GroupProperties> getGroupProperties(String scope, String group) {
        return store.getGroupProperties(scope, group);
    }

    /**
     * Update group's schema validation policy. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param validationRules New validation rules for the group.
     * @return CompletableFuture which is completed when validation policy update completes.
     */
    public CompletableFuture<Void> updateSchemaValidationPolicy(String scope, String group, SchemaValidationRules validationRules) {
        return Futures.toVoid(store.getGroupEtag(scope, group)
                .thenCompose(etag -> store.updateCompatibilityPolicy(scope, group, etag, validationRules)));
    }

    /**
     * Gets list of subgroups registered under the group. Subgroups are identified by {@link SchemaInfo#name}
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param token Continuation token.
     * @return CompletableFuture which holds list of subgroups upon completion. 
     * If group is configured to store schemas in subgroups then subgroups are returned. Otherwise an empty list is returned.
     */
    public CompletableFuture<ListWithToken<String>> getSubgroups(String scope, String group, ContinuationToken token) {
        return store.listSubGroups(scope, group, token);
    }

    /**
     * Adds schema to the group. If group is configured to include schemas by event type in subgroups, then 
     * the {@link SchemaInfo#name} is used to store schema in the subgroup. 
     * Schema validation rules that are sent to the registry should be a super set of Validation rules set in 
     * {@link GroupProperties#schemaValidationRules}
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema Schema to add. 
     * @param rules Schema validation rules to apply. 
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.   
     */
    public CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String group, SchemaInfo schema, SchemaValidationRules rules) {
        // 1. get group policy
        // 2. get checker for schema type.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        CompatibilityChecker checker;
        
        return store.getGroupEtag(scope, group)
            .thenCompose(etag -> store.getGroupProperties(scope, group)
                         .thenCompose(prop -> {
                             SchemaValidationRules policy = prop.getSchemaValidationRules();
                             if (prop.isSubgroupBySchemaName()) {
                                 String subgroup = schema.getName();
                                 // todo: apply policy
                                 // get schemas for subgroup for validation
                                 return store.conditionallyAddSchemaToSubgroup(scope, group, subgroup, etag, schema);
                             } else {
                                 // todo: apply policy
                                 // get schemas for group for validation
                                 return store.conditionallyAddSchemaToGroup(scope, group, etag, schema);
                             }
                         }));
    }

    /**
     * Gets schema corresponding to the version. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return CompletableFuture that holds Schema info corresponding to the version info. 
     */
    public CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo version) {
        return store.getSchema(scope, group, version);
    }

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     *
     * @param scope Name of scope.
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id. 
     */
    public CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId) {
        return store.getEncodingInfo(scope, group, encodingId);
    }

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return CompletableFuture that holds Encoding id for the pair of version and compression type.
     */
    public CompletableFuture<EncodingId> getEncodingId(String scope, String group, VersionInfo version, CompressionType compressionType) {
        return store.createOrGetEncodingId(scope, group, version, compressionType);
    }

    /**
     * Gets latest schema and version for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param subgroup Name of subgroup. 
     *
     * @return CompletableFuture that holds Schema with version for the last schema that was added to the group (or subgroup).
     */
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, @Nullable String subgroup) {
        if (subgroup == null) {
            return store.getLatestSchema(scope, group);
        } else {
            return store.getLatestSchema(scope, group, subgroup);
        }
    }

    /**
     * Gets all schemas with corresponding versions for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. {@link SchemaInfo#name} is used as the subgroup name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     *
     * @param scope Name of scope. 
     * @param group Name of group.
     * @param subgroup Name of subgroup. 
     * @return CompletableFuture that holds Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    public CompletableFuture<List<SchemaEvolutionEpoch>> getGroupEvolutionHistory(String scope, String group, @Nullable String subgroup) {
        return store.getGroupProperties(scope, group)
                .thenCompose(prop -> {
                    if (prop.isSubgroupBySchemaName()) {
                        if (subgroup == null) {
                            throw new InputMismatchException();
                        } 
                        return store.getSubGroupHistory(scope, group, subgroup);
                    } else {
                        return store.getGroupHistory(scope, group);
                    }
                });
    }

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#subgroupBySchemaName}
     * the subgroup name is taken from the SchemaInfo. 
     * Version is uniquely identified by {@link SchemaInfo#schemaData}. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return CompletableFuture that holds VersionInfo corresponding to schema. 
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schema) {
        return store.getSchemaVersion(scope, group, schema);
    }

    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group (/subgroup) 
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema Schema to check for validity. 
     * @param validationRules validation rules to apply.
     * @return True if it satisfies validation checks, false otherwise. 
     */
    public CompletableFuture<Boolean> validateSchema(String scope, String group, SchemaInfo schema, SchemaValidationRules validationRules) {
        // based on compatibility policy, fetch specific schemas for the group/subgroup and perform validations
        // TODO: validate schema
        
        return store.getGroupProperties(scope, group)
                .thenCompose(prop -> {
                    if (prop.isSubgroupBySchemaName()) {
                        // TODO: based on policy fetch a subset of history
                        return store.getSubGroupHistory(scope, group, schema.getName())
                                .thenApply(history -> {
                                    // validate against policy and history
                                    return true;
                                });
                    } else {
                        return store.getGroupHistory(scope, group)
                                .thenApply(history -> {
                                    // validate against policy and history
                                    return true;
                                });
                    }
                });
    }

    /**
     * Deletes group.  
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @return CompletableFuture which is completed when group is deleted. 
     */
    public CompletableFuture<Void> deleteGroup(String scope, String group) {
        return store.deleteGroup(scope, group);
    }

    /**
     * List of compressions used for encoding in the group. This will be returned only if {@link GroupProperties#enableEncoding}
     * is set to true. 
     *
     * @param scope Name of scope.
     * @param group Name of group. 
     * @return CompletableFuture that holds list of compressions used for encoding in the group. 
     */
    public CompletableFuture<List<CompressionType>> getCompressions(String scope, String group) {
        return store.getCompressions(scope, group);
    }
}
