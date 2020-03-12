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
import java.util.Map;

/**
 * Defines a registry client for interacting with schema registry service. 
 */
public interface SchemaRegistryClient {
    /**
     * Adds a new group. Refer to {@link GroupProperties} for details about each property. 
     * 
     * @param group Name of group that uniquely identifies the group. 
     * @param schemaType Serialization format used to encode data in the group. 
     * @param validationRules Schema validation policy to apply for the group. 
     * @param validateByObjectType Property to describe whether group should have schema evolution checks performed by object types. 
     *                            Object Types are uniquely identified by {@link SchemaInfo#name}. 
     * @param properties          Map of properties. Example properties could include flag to indicate whether client should
     *                            encode registry service generated encoding id with payload. 
     * @return True indicates if the group was added successfully, false if it exists. 
     */
    boolean addGroup(String group, SchemaType schemaType, SchemaValidationRules validationRules,
                     boolean validateByObjectType, Map<String, String> properties);
    
    /**
     * Api to remove group. 
     * 
     * @param group Name of group that uniquely identifies the group. 
     */
    void removeGroup(String group);

    /**
     * List all groups. 
     * 
     * @return map of names of groups with corresponding group properties for all groups. 
     */
    Map<String, GroupProperties> listGroups();
    
    /**
     * Gets group's properties. 
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#validateByObjectType} that specifies if schemas should be exclusively validated against 
     * schemas that have the same {@link SchemaInfo#name}. 
     * {@link GroupProperties#properties} describes generic properties for a group.
     * 
     * @param group Name of group.
     * @return Group properties which includes property like Schema Type and compatibility policy. 
     */
    GroupProperties getGroupProperties(String group);

    /**
     * Update group's schema validation policy. 
     * 
     * @param group Name of group. 
     * @param validationRules New compatibility setting for the group.
     */
    void updateSchemaValidationRules(String group, SchemaValidationRules validationRules);

    /**
     * Add new rule to {@link SchemaValidationRules}. 
     * 
     * @param group Name of group. 
     * @param rule SchemaValidationRule to add
     */
    void addSchemaValidationRule(String group, SchemaValidationRule rule);

    /**
     * Remove rule from {@link SchemaValidationRules}.
     * 
     * @param group Name of group. 
     * @param rule SchemaValidationRule to remove
     */
    void removeSchemaValidationRule(String group, SchemaValidationRule rule);
    
    /**
     * Gets list of object types registered under the group. ObjectTypes are identified by {@link SchemaInfo#name}
     * 
     * @param group Name of group. 
     * @return List of objectTypes within the group.   
     */
    List<String> getObjectTypes(String group);

    /**
     * Adds schema to the group. If group is configured with {@link GroupProperties#validateByObjectType} then 
     * the {@link SchemaInfo#name} is used for validating against existing group schemas that share the same name. 
     * Schema validation rules that are sent to the registry should be a super set of Validation rules set in 
     * {@link GroupProperties#schemaValidationRules}
     * 
     * @param group Name of group. 
     * @param schema Schema to add. 
     *              
     * @return versionInfo which uniquely identifies where the schema is added in the group.   
     */
    VersionInfo addSchemaIfAbsent(String group, SchemaInfo schema);

    /**
     * Gets schema corresponding to the version. 
     * 
     * @param group Name of group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return Schema info corresponding to the version info. 
     */
    SchemaInfo getSchema(String group, VersionInfo version);

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType. 
     * 
     * @param group Name of group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return Encoding info corresponding to the encoding id. 
     */
    EncodingInfo getEncodingInfo(String group, EncodingId encodingId);

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type. 
     * 
     * @param group Name of group. 
     * @param version version of schema 
     * @param compressionType compression type
     * @return Encoding id for the pair of version and compression type.
     */
    EncodingId getEncodingId(String group, VersionInfo version, CompressionType compressionType);

    /**
     * Gets latest schema and version for the group (or objectTypeName, if specified). 
     * For groups configured with {@link GroupProperties#validateByObjectType}, the objectTypeName name needs to be supplied to 
     * get the latest schema for the objectTypeName. 
     * 
     * @param group Name of group. 
     * @param objectTypeName Name of objectTypeName. 
     *                 
     * @return Schema with version for the last schema that was added to the group (or objectTypeName).
     */
    SchemaWithVersion getLatestSchema(String group, @Nullable String objectTypeName);

    /**
     * Gets all schemas with corresponding versions for the group (or objectTypeName, if specified). 
     * For groups configured with {@link GroupProperties#validateByObjectType}, the objectTypeName name needs to be supplied to 
     * get the latest schema for the objectTypeName. {@link SchemaInfo#name} is used as the objectTypeName name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     * 
     * @param group Name of group.
     * @param objectTypeName Name of objectTypeName. 
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    List<SchemaEvolution> getGroupEvolutionHistory(String group, @Nullable String objectTypeName);

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#validateByObjectType}
     * the version will contain the schemaName taken from the {@link SchemaInfo#name}. 
     * Version is uniquely identified by {@link SchemaInfo#schemaData}. 
     * 
     * @param group Name of group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return VersionInfo corresponding to schema. 
     */
    VersionInfo getSchemaVersion(String group, SchemaInfo schema);
    
    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * 
     * @param group Name of group. 
     * @param schema Schema to check for validity. 
     * @return True if it satisfies validation checks, false otherwise. 
     */
    boolean validateSchema(String group, SchemaInfo schema);

    /**
     * Checks whether given schema can be used to read by validating it for reads against one or more existing schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * 
     * @param group Name of group. 
     * @param schema Schema to check to be used for reads. 
     * @return True if it can be used to read, false otherwise. 
     */
    boolean canRead(String group, SchemaInfo schema);

    /**
     * List of compressions used for encoding in the group. 
     * 
     * @param group Name of group. 
     * @return List of compressions used for encoding in the group. 
     */
    List<CompressionType> getCompressions(String group);
}
