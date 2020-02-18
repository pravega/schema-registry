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
import io.pravega.schemaregistry.storage.ContinuationToken;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SchemaRegistryService {
    /**
     * Lists all scopes.
     * 
     * @param token Continuation Token. 
     * @return CompletableFuture which holds list of scope names upon completion. 
     */
    CompletableFuture<ListWithToken<String>> listScopes(String token);

    /**
     * Creates new scope if absent. This api is idempotent. 
     * @param scope Name of scope.
     * @return CompletableFuture which is completed when create scope completes.
     */
    CompletableFuture<Void> createScope(String scope);

    /**
     * Deletes a scope.  
     * @param scope Name of scope.
     * @return CompletableFuture which is completed when delete scope completes.
     */
    CompletableFuture<Void> deleteScope(String scope);

    /**
     * Lists groups in scope. 
     * 
     * @param scope Name of scope.
     * @param continuationToken continuation token
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    CompletableFuture<MapWithToken<String, GroupProperties>> listGroupsInScope(String scope, String continuationToken);

    /**
     * Creates new group within a scope. Idempotent behaviour. If group already exists, it returns false. 
     * 
     * @param scope Name of scope.
     * @param group Name of group. 
     * @param groupProperties Group properties.
     * @return CompletableFuture which is completed when create group completes. True indicates this was 
     * new group, false indicates it was an existing group. 
     */
    CompletableFuture<Boolean> createGroup(String scope, String group, GroupProperties groupProperties);

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
    CompletableFuture<GroupProperties> getGroupProperties(String scope, String group);

    /**
     * Update group's schema validation policy. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param validationRules New validation rules for the group.
     * @return CompletableFuture which is completed when validation policy update completes.
     */
    CompletableFuture<Void> updateSchemaValidationPolicy(String scope, String group, SchemaValidationRules validationRules);

    /**
     * Gets list of subgroups registered under the group. Subgroups are identified by {@link SchemaInfo#name}
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param token Continuation token.
     * @return CompletableFuture which holds list of subgroups upon completion. 
     * If group is configured to store schemas in subgroups then subgroups are returned. Otherwise an empty list is returned.
     */
    CompletableFuture<ListWithToken<String>> getSubgroups(String scope, String group, ContinuationToken token);

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
    CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String group, SchemaInfo schema,
                                                     SchemaValidationRules rules);

    /**
     * Gets schema corresponding to the version. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return CompletableFuture that holds Schema info corresponding to the version info. 
     */
    CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo version);

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     *
     * @param scope Name of scope.
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id. 
     */
    CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId);

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     *
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return CompletableFuture that holds Encoding id for the pair of version and compression type.
     */
    CompletableFuture<EncodingId> getEncodingId(String scope, String group, VersionInfo version, 
                                                CompressionType compressionType);

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
    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, @Nullable String subgroup);

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
    CompletableFuture<List<SchemaEvolutionEpoch>> getGroupEvolutionHistory(String scope, String group, @Nullable String subgroup);

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
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schema);
    
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
    CompletableFuture<Boolean> validateSchema(String scope, String group, SchemaInfo schema, SchemaValidationRules validationRules);

    /**
     * Deletes group.  
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @return CompletableFuture which is completed when group is deleted. 
     */
    CompletableFuture<Void> deleteGroup(String scope, String group);

    /**
     * List of compressions used for encoding in the group. This will be returned only if {@link GroupProperties#enableEncoding}
     * is set to true. 
     *
     * @param scope Name of scope.
     * @param group Name of group. 
     * @return CompletableFuture that holds list of compressions used for encoding in the group. 
     */
    CompletableFuture<List<CompressionType>> getCompressions(String scope, String group);
}
