/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Defines a registry client for interacting with schema registry service. 
 */
public interface SchemaRegistryClient {
    /**
     * Creates new namespace with specified name if absent. This api is idempotent. 
     * 
     * @param namespace Name of namespace.
     */
    void createNamespace(String namespace);

    /**
     * Deletes a namespace with specified name.
     * 
     * @param namespace Name of namespace.
     */
    void deleteNamespace(String namespace);

    /**
     * Adds a new group to the specified namespace. Refer to {@link GroupProperties} for details about each property. 
     * 
     * @param namespace Name of the namespace to add the group to. 
     * @param group Name of group that uniquely identifies the group within a namespace. 
     * @param schemaType Serialization format used to encode data in the group. 
     * @param validationRules Schema validation policy to apply for the group. 
     * @param subgroupBySchemaName Property to describe whether group should be subdivided into sub groups by event types. 
     *                            Event Types are uniquely identified by {@link SchemaInfo#name}. 
     * @param enableEncoding Property that indicates whether registry service should generating an encoding id. If 
     *                       set to false, {@link EncodingInfo} and {@link EncodingId} are not generated for schemas in 
     *                       the group. 
     * @return True indicates if the group was added successfully, false if it exists. 
     */
    boolean addGroup(String namespace, String group, SchemaType schemaType, SchemaValidationRules validationRules,
                     boolean subgroupBySchemaName, boolean enableEncoding);
    
    /**
     * Remove group from the specified namespace. 
     * 
     * @param namespace Name of the namespace to add the group to. 
     * @param group Name of group that uniquely identifies the group within a namespace. 
     */
    void removeGroup(String namespace, String group);

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
     * @return Group properties which includes property like Schema Type and compatibility policy. 
     */
    GroupProperties getGroupProperties(String namespace, String group);

    /**
     * Update group's schema validation policy. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param validationRules New compatibility setting for the group.
     */
    void updateSchemaValidationRules(String namespace, String group, SchemaValidationRules validationRules);

    /**
     * Add new rule to {@link SchemaValidationRules}. 
     * 
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @param rule Rule to add
     */
    void addSchemaValidationRule(String namespace, String group, SchemaValidationRule rule);

    /**
     * Remove rule from {@link SchemaValidationRules}.
     * 
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @param rule Rule to remove
     */
    void removeSchemaValidationRule(String namespace, String group, SchemaValidationRule rule);
    
    /**
     * Gets list of subgroups registered under the group. Subgroups are identified by {@link SchemaInfo#name}
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @return List of subgroups within the group. If group is configured to store schemas in subgroups then 
     *      * subgroups are returned. Otherwise an empty list is returned.  
     */
    List<String> getSubgroups(String namespace, String group);

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
     *              
     * @return versionInfo which uniquely identifies where the schema is added in the group.   
     */
    VersionInfo addSchemaIfAbsent(String namespace, String group, SchemaInfo schema, SchemaValidationRules rules);

    /**
     * Gets schema corresponding to the version. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return Schema info corresponding to the version info. 
     */
    SchemaInfo getSchema(String namespace, String group, VersionInfo version);

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     * 
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return Encoding info corresponding to the encoding id. 
     */
    EncodingInfo getEncodingInfo(String namespace, String group, EncodingId encodingId);

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return Encoding id for the pair of version and compression type.
     */
    EncodingId getEncodingId(String namespace, String group, VersionInfo version, CompressionType compressionType);

    /**
     * Gets latest schema and version for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param subgroup Name of subgroup. 
     *                 
     * @return Schema with version for the last schema that was added to the group (or subgroup).
     */
    SchemaWithVersion getLatestSchema(String namespace, String group, @Nullable String subgroup);

    /**
     * Gets all schemas with corresponding versions for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupBySchemaName}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. {@link SchemaInfo#name} is used as the subgroup name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group.
     * @param subgroup Name of subgroup. 
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    List<SchemaEvolution> getGroupEvolutionHistory(String namespace, String group, @Nullable String subgroup);

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#subgroupBySchemaName}
     * the subgroup name is taken from the SchemaInfo. 
     * Version is uniquely identified by {@link SchemaInfo#schemaData}. 
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return VersionInfo corresponding to schema. 
     */
    VersionInfo getSchemaVersion(String namespace, String group, SchemaInfo schema);
    
    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group (/subgroup) 
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * 
     * @param namespace Name of namespace. 
     * @param group Name of group. 
     * @param schema Schema to check for validity. 
     * @param validationRules validation rules to apply.
     * @return True if it satisfies validation checks, false otherwise. 
     */
    boolean validateSchema(String namespace, String group, SchemaInfo schema, SchemaValidationRules validationRules);

    /**
     * List of compressions used for encoding in the group. This will be returned only if {@link GroupProperties#enableEncoding}
     * is set to true. 
     * 
     * @param namespace Name of namespace.
     * @param group Name of group. 
     * @return List of compressions used for encoding in the group. 
     */
    List<CompressionType> getCompressions(String namespace, String group);
}
