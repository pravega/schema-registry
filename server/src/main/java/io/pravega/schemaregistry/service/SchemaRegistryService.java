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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.rules.CompatibilityChecker;
import io.pravega.schemaregistry.rules.CompatibilityCheckerFactory;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;

import javax.annotation.Nullable;
import java.util.Collections;
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
     * Lists all namespaces.
     *
     * @param continuationToken Continuation Token. 
     * @return CompletableFuture which holds list of namespace names upon completion. 
     */
    public CompletableFuture<ListWithToken<String>> listNamespaces(String continuationToken) {
        return store.listNamespaces(ContinuationToken.parse(continuationToken));
    }

    /**
     * Creates new namespace if absent. This api is idempotent. 
     * @param namespace Name of namespace.
     * @return CompletableFuture which is completed when create namespace completes.
     */
    public CompletableFuture<Void> createNamespace(String namespace) {
        return store.createNamespace(namespace);
    }

    /**
     * Deletes a namespace.  
     * @param namespace Name of namespace.
     * @return CompletableFuture which is completed when delete namespace completes.
     */
    public CompletableFuture<Void> deleteNamespace(String namespace) {
        return store.deleteNamespace(namespace);
    }

    /**
     * Lists groups in namespace. 
     *
     * @param namespace Name of namespace.
     * @param continuationToken continuation token
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroupsInNamespace(String namespace, String continuationToken) {
        return store.listGroups(namespace, ContinuationToken.parse(continuationToken))
                .thenCompose(reply -> {
                    ContinuationToken token = reply.getToken();
                    List<String> list = reply.getList();
                    return Futures.allOfWithResults(list.stream().collect(Collectors.toMap(x -> x, x -> store.getGroupProperties(namespace, x))))
                           .thenApply(groups -> new MapWithToken<>(groups, token));
                });
    }

    /**
     * Creates new group within a namespace. Idempotent behaviour. If group already exists, it returns false. 
     *
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @param groupProperties Group properties.
     * @return CompletableFuture which is completed when create group completes. True indicates this was 
     * new group, false indicates it was an existing group. 
     */
    public CompletableFuture<Boolean> createGroup(String namespace, String group, GroupProperties groupProperties) {
        Preconditions.checkNotNull(groupProperties.getSchemaType());
        Preconditions.checkNotNull(groupProperties.getSchemaValidationRules());
        Preconditions.checkArgument(validateRules(groupProperties, groupProperties.getSchemaValidationRules()));
        return store.createGroupInNamespace(namespace, group, groupProperties);
    }

    private boolean validateRules(GroupProperties groupProperties, SchemaValidationRules newRules) {
        Preconditions.checkNotNull(newRules);
        Compatibility.Type compatibility = newRules.getCompatibility().getCompatibility();
        boolean subgroupCheck = !groupProperties.isSubgroupBySchemaName() || 
                isValidSubgroupPolicy(compatibility);
        
        switch (groupProperties.getSchemaType().getSchemaType()) {
            case Avro:
                return subgroupCheck;
            case Protobuf:
            case Json:
            case Custom:
                return subgroupCheck && isValidNonAvroPolicy(compatibility);
        } 
        return true;
    }

    private boolean isValidSubgroupPolicy(Compatibility.Type compatibility) {
        return !(compatibility.equals(Compatibility.Type.BackwardTill) ||
                compatibility.equals(Compatibility.Type.ForwardTill) ||
                compatibility.equals(Compatibility.Type.BackwardTillAndForwardTill));
    }
    
    private boolean isValidNonAvroPolicy(Compatibility.Type compatibility) {
        return compatibility.equals(Compatibility.Type.AllowAny) ||
                compatibility.equals(Compatibility.Type.DisallowAll);
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
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @return CompletableFuture which holds group properties upon completion. 
     */
    public CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group) {
        return store.getGroupProperties(namespace, group);
    }

    /**
     * Update group's schema validation policy. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param validationRules New validation rules for the group.
     * @return CompletableFuture which is completed when validation policy update completes.
     */
    public CompletableFuture<Void> updateSchemaValidationPolicy(String namespace, String group, SchemaValidationRules validationRules) {
        return store.getGroupEtag(namespace, group)
                    .thenCompose(pos -> store.getGroupProperties(namespace, group)
                                             .thenCompose(groupProperties -> {
                                                 if (validateRules(groupProperties, validationRules)) {
                                                     return store.updateValidationPolicy(namespace, group, pos, validationRules);
                                                 } else {
                                                     throw new IllegalArgumentException();
                                                 }
                                             }));
    }

    /**
     * Gets list of subgroups registered under the group. Subgroups are identified by {@link SchemaInfo#name}
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @return CompletableFuture which holds list of subgroups upon completion. 
     * If group is configured to store schemas in subgroups then subgroups are returned. Otherwise an empty list is returned.
     */
    public CompletableFuture<ListWithToken<String>> getSubgroups(String namespace, String group) {
        return store.listSubGroups(namespace, group);
    }

    /**
     * Adds schema to the group. If group is configured to include schemas by event type in subgroups, then 
     * the {@link SchemaInfo#name} is used to store schema in the subgroup. 
     * Schema validation rules that are sent to the registry should be a super set of Validation rules set in 
     * {@link GroupProperties#schemaValidationRules}
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param schema Schema to add. 
     * @param rules Schema validation rules to apply. 
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.   
     */
    public CompletableFuture<VersionInfo> addSchemaIfAbsent(String namespace, String group, SchemaInfo schema, SchemaValidationRules rules) {
        // 1. get group policy
        // 2. get checker for schema type.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        return store.getGroupEtag(namespace, group)
            .thenCompose(etag -> store.getGroupProperties(namespace, group)
                         .thenCompose(prop -> {
                             return Futures.exceptionallyComposeExpecting(store.getSchemaVersion(namespace, group, schema),
                             e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, () -> {
                                         if (prop.isSubgroupBySchemaName()) {
                                             String subgroup = schema.getName();
                                             
                                             // subgroup policy cannot be backward till forward till etc. 
                                             // get schemas for subgroup for validation
                                             return store.addSchemaToSubgroup(namespace, group, subgroup, etag, schema);
                                         } else {
                                             // todo: apply policy
                                             // get schemas for group for validation
                                             return store.addSchemaToGroup(namespace, group, etag, schema);
                                         }
                                     });
                         }));
    }

    /**
     * Gets schema corresponding to the version. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return CompletableFuture that holds Schema info corresponding to the version info. 
     */
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String group, VersionInfo version) {
        return store.getSchema(namespace, group, version);
    }

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     *
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id. 
     */
    public CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId) {
        return store.getEncodingInfo(namespace, group, encodingId);
    }

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return CompletableFuture that holds Encoding id for the pair of version and compression type.
     */
    public CompletableFuture<EncodingId> getEncodingId(String namespace, String group, VersionInfo version, CompressionType compressionType) {
        return store.getGroupProperties(namespace, group)
             .thenCompose(prop -> {
                 if (prop.isSubgroupBySchemaName()) {
                     return store.getLatestSchema(namespace, group, version.getSchemaName());
                 } else {
                     return store.getLatestSchema(namespace, group);
                 }
             }).thenApply(latest -> {
                 // TODO: based on compatibility type either allow or deny the version
                    
        }).thenCompose(v -> store.createOrGetEncodingId(namespace, group, version, compressionType));
    }

    /**
     * Gets latest schema and version for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param subgroup Name of subgroup. 
     *
     * @return CompletableFuture that holds Schema with version for the last schema that was added to the group (or subgroup).
     */
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String namespace, String group, @Nullable String subgroup) {
        if (subgroup == null) {
            return store.getLatestSchema(namespace, group);
        } else {
            return store.getLatestSchema(namespace, group, subgroup);
        }
    }

    /**
     * Gets all schemas with corresponding versions for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. {@link SchemaInfo#name} is used as the subgroup name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group.
     * @param subgroup Name of subgroup. 
     * @return CompletableFuture that holds Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    public CompletableFuture<List<SchemaEvolution>> getGroupEvolutionHistory(String namespace, String group, @Nullable String subgroup) {
        return store.getGroupProperties(namespace, group)
                .thenCompose(prop -> {
                    if (prop.isSubgroupBySchemaName()) {
                        if (subgroup == null) {
                            throw new InputMismatchException();
                        } 
                        return store.getSubGroupHistory(namespace, group, subgroup);
                    } else {
                        return store.getGroupHistory(namespace, group);
                    }
                }).thenApply(ListWithToken::getList);
    }

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#subgroupBySchemaName}
     * the subgroup name is taken from the SchemaInfo. 
     * Version is uniquely identified by {@link SchemaInfo#schemaData}. 
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return CompletableFuture that holds VersionInfo corresponding to schema. 
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schema) {
        return store.getSchemaVersion(namespace, group, schema);
    }

    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group (/subgroup) 
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     *
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param schema Schema to check for validity. 
     * @param rules validation rules to apply.
     * @return True if it satisfies validation checks, false otherwise. 
     */
    public CompletableFuture<Boolean> validateSchema(String namespace, String group, SchemaInfo schema, SchemaValidationRules rules) {
        // based on compatibility policy, fetch specific schemas for the group/subgroup and perform validations
        // TODO: validate schema
        
        return store.getGroupProperties(namespace, group)
                .thenCompose(prop -> {
                    return getSchemasForValidation(namespace, group, schema, prop, rules)
                        .thenCompose(schemas -> {
                            checkCompatibility()
                        });        
                });
    }

    private CompletableFuture<List<SchemaWithVersion>> getSchemasForValidation(String namespace, String group, SchemaInfo schema, 
                                                      GroupProperties groupProperties, SchemaValidationRules additionalRules) {
        CompletableFuture<List<SchemaWithVersion>> schemasFuture;
        switch (groupProperties.getSchemaValidationRules().getCompatibility().getCompatibility()) {
            case DisallowAll:
            case AllowAny:
                schemasFuture = CompletableFuture.completedFuture(Collections.emptyList());
            case Backward:
            case Forward:
            case Full:
                // get latest schema
                if (groupProperties.isSubgroupBySchemaName()) {
                    schemasFuture = store.getLatestSchema(namespace, group, schema.getName())
                            .thenApply(Collections::singletonList);
                } else {
                    schemasFuture = store.getLatestSchema(namespace, group)
                                         .thenApply(Collections::singletonList);
                }
                break;
            case BackwardTransitive:
            case ForwardTransitive:
            case FullTransitive:
                // get all schemas
                if (groupProperties.isSubgroupBySchemaName()) {
                    schemasFuture = store.listSchemasInSubgroup(namespace, group, schema.getName())
                                         .thenApply(ListWithToken::getList);
                } else {
                    schemasFuture = store.listSchemasInGroup(namespace, group)
                                         .thenApply(ListWithToken::getList);
                }
                break;
            case BackwardTill:
                // get schema till
                assert !groupProperties.isSubgroupBySchemaName();
                assert groupProperties.getSchemaValidationRules().getCompatibility().getBackwardTill() != null;
                schemasFuture = store.listSchemasInGroup(namespace, group, 
                        groupProperties.getSchemaValidationRules().getCompatibility().getBackwardTill())
                                     .thenApply(ListWithToken::getList);
                break;
            case ForwardTill:
                // get schema till
                assert !groupProperties.isSubgroupBySchemaName();
                assert groupProperties.getSchemaValidationRules().getCompatibility().getForwardTill() != null;
                schemasFuture = store.listSchemasInGroup(namespace, group, 
                        groupProperties.getSchemaValidationRules().getCompatibility().getForwardTill())
                                     .thenApply(ListWithToken::getList);
                break;
            case BackwardTillAndForwardTill:
                assert !groupProperties.isSubgroupBySchemaName();
                assert groupProperties.getSchemaValidationRules().getCompatibility().getBackwardTill() != null;
                assert groupProperties.getSchemaValidationRules().getCompatibility().getForwardTill() != null;
                VersionInfo backwardTill = groupProperties.getSchemaValidationRules().getCompatibility().getBackwardTill();
                VersionInfo forwardTill = groupProperties.getSchemaValidationRules().getCompatibility().getForwardTill();
                VersionInfo versionTill = backwardTill.getVersion() < forwardTill.getVersion() ? backwardTill : forwardTill; 
                schemasFuture = store.listSchemasInGroup(namespace, group, versionTill)
                                     .thenApply(ListWithToken::getList);
                break;
            default:
                throw new IllegalArgumentException();
        }
        
        return schemasFuture
                .thenApply(schemas -> checkCompatibility(namespace, group, schema, groupProperties, additionalRules, schemas));
    }

    private boolean checkCompatibility(String namespace, String group, SchemaInfo schema, GroupProperties groupProperties, 
                                       SchemaValidationRules additionalRules, List<SchemaWithVersion> schemas) {
        CompatibilityChecker checker = CompatibilityCheckerFactory.getCompatibilityChecker(schema.getSchemaType());

        return false;
    }


    /**
     * Deletes group.  
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @return CompletableFuture which is completed when group is deleted. 
     */
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        return store.deleteGroup(namespace, group);
    }

    /**
     * List of compressions used for encoding in the group. This will be returned only if {@link GroupProperties#enableEncoding}
     * is set to true. 
     *
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @return CompletableFuture that holds list of compressions used for encoding in the group. 
     */
    public CompletableFuture<List<CompressionType>> getCompressions(String namespace, String group) {
        return store.getCompressions(namespace, group);
    }
}
