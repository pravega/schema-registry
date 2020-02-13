/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import io.pravega.schemaregistry.contract.Compatibility;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingId;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.GroupProperties;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaWithVersion;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaValidationRules;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Defines a registry client for interacting with schema registry service. 
 */
public interface SchemaRegistryClient {
    /**
     * Adds a new group under the specified scope. 
     * 
     * @param scope Name of the scope to add the group to. 
     * @param group Name of group that uniquely identifies the group within a scope. 
     * @param schemaType Serialization format used to encode data in the group. 
     * @param compatibility Compatibility policy to apply for the group. 
     * @param subgroupByEventType Property to describe whether group should be subdivided into sub groups by event types. 
     *                            Event Types are uniquely identified by {@link SchemaInfo#name}. 
     * @param enableEncoding Property that indicates whether registry service should generating an encoding id. If 
     *                       set to false, {@link EncodingInfo} and {@link EncodingId} are not generated for schemas in 
     *                       the group. 
     * @return True indicates if the group was added successfully, false if it exists. 
     */
    boolean addGroup(String scope, String group, SchemaType schemaType, Compatibility compatibility, 
                     boolean subgroupByEventType, boolean enableEncoding);

    /**
     * Gets group's properties. 
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#compatibility} sets the compatibility policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#subgroupByEventType} that specifies if schemas are subgrouped by event type. 
     * Event Types are uniquely identified by {@link SchemaInfo#name}. 
     * {@link GroupProperties#enableEncoding} describes whether registry should generate encoding ids to identify 
     * encoding properties in {@link EncodingInfo}.
     * 
     * @param scope Name of scope. 
     * @param group Name of group.
     * @return Group properties which includes property like Schema Type and compatibility policy. 
     */
    GroupProperties getGroupProperties(String scope, String group);

    /**
     * Update group's compatibility policy. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param compatibility New compatibility setting for the group.
     */
    void updateCompatibilityPolicy(String scope, String group, Compatibility compatibility);

    /**
     * Gets list of subgroups registered under the group. Subgroups are identified by {@link SchemaInfo#name}
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @return List of subgroups within the group. If group is configured to store schemas in subgroups then 
     *      * subgroups are returned. Otherwise an empty list is returned.  
     */
    List<String> getSubgroups(String scope, String group);

    /**
     * Adds schema to the group. If group is configured to include schemas by event type in subgroups, then 
     * the {@link SchemaInfo#name} is used to store schema in the subgroup. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema Schema to add. 
     * @param rules Schema validation rules to apply. 
     *              
     * @return versionInfo which uniquely identifies where the schema is added in the group.   
     */
    VersionInfo addSchemaIfAbsent(String scope, String group, SchemaInfo schema, SchemaValidationRules rules);

    /**
     * Gets schema corresponding to the version. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return Schema info corresponding to the version info. 
     */
    SchemaInfo getSchema(String scope, String group, VersionInfo version);

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     * 
     * @param scope Name of scope.
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return Encoding info corresponding to the encoding id. 
     */
    EncodingInfo getEncodingInfo(String scope, String group, EncodingId encodingId);

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return Encoding id for the pair of version and compression type.
     */
    EncodingId getEncodingId(String scope, String group, VersionInfo version, CompressionType compressionType);

    /**
     * Gets latest schema and version for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupByEventType}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param subgroup Name of subgroup. 
     *                 
     * @return Schema with version for the last schema that was added to the group (or subgroup).
     */
    SchemaWithVersion getLatestSchema(String scope, String group, @Nullable String subgroup);

    /**
     * Gets all schemas with corresponding versions for the group (or subgroup, if specified). 
     * For groups configured with {@link GroupProperties#subgroupByEventType}, the subgroup name needs to be supplied to 
     * get the latest schema for the subgroup. {@link SchemaInfo#name} is used as the subgroup name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group.
     * @param subgroup Name of subgroup. 
     * @return Ordered list of schemas with versions for all schemas in the group. 
     */
    List<SchemaWithVersion> getAllSchemas(String scope, String group, @Nullable String subgroup);

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#subgroupByEventType}
     * the subgroup name is taken from the SchemaInfo. 
     * Version is uniquely identified by {@link SchemaInfo#schemaDataBase64}. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return VersionInfo corresponding to schema. 
     */
    VersionInfo getSchemaVersion(String scope, String group, SchemaInfo schema);

    /**
     * Checks whether schema identified by readVersion can read data serialized using schema identified by writeVersion.
     * The actual check depends on {@link SchemaType}. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param subgroup Name of subgroup. 
     * @param writeVersion Version for writer schema. 
     * @param readVersion Version for reader schema. 
     * @return True if it can be read, false otherwise. 
     */
    boolean canRead(String scope, String group, @Nullable String subgroup, VersionInfo writeVersion, VersionInfo readVersion);

    /**
     * Checks whether given schema is compatible with previous schemas in the group (/subgroup) subject to current {@link Compatibility}
     * policy configured for the group. 
     * 
     * @param scope Name of scope. 
     * @param group Name of group. 
     * @param schema Schema to check compatibility for. 
     * @return True if it satifies compatibility checks, false otherwise. 
     */
    boolean checkCompatibility(String scope, String group, SchemaInfo schema);
}
