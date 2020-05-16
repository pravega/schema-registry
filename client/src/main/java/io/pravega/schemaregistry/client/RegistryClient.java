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

import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Defines a registry client for interacting with schema registry service. 
 */
public interface RegistryClient {
    /**
     * Adds a new group. A group refers to the name under which the schemas are registered. A group is identified by a 
     * unique name and has an associated set of group metadata {@link GroupProperties} and a list of codecs and a 
     * versioned history of schemas that were registered under the group. 
     * 
     * @param groupId Id for the group that uniquely identifies the group. 
     * @param schemaType Serialization format used to encode data in the group. 
     * @param validationRules Schema validation policy to apply for the group. 
     * @param versionBySchemaName Property to describe whether schema compatibility checks should performed for schemas that 
     *                            share same {@link SchemaInfo#name}. Schema names identify objects of same type.  
     * @param properties          Map of properties. Example properties could include flag to indicate whether client should
     *                            encode registry service generated encoding id with payload. 
     * @return True indicates if the group was added successfully, false if it exists. 
     */
    boolean addGroup(String groupId, SchemaType schemaType, SchemaValidationRules validationRules,
                     boolean versionBySchemaName, Map<String, String> properties);
    
    /**
     * Remove group. 
     * 
     * @param groupId Id for the group that uniquely identifies the group. 
     */
    void removeGroup(String groupId);

    /**
     * List all groups. 
     * 
     * @return map of names of groups with corresponding group properties for all groups. 
     */
    Map<String, GroupProperties> listGroups();
    
    /**
     * Get group properties for the group. 
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#versionBySchemaName} that specifies if schemas should be exclusively validated against 
     * schemas that have the same {@link SchemaInfo#name}. 
     * {@link GroupProperties#properties} describes generic properties for a group.
     * 
     * @param groupId Id for the group.
     * @return Group properties which includes property like Schema Type and compatibility policy. 
     */
    GroupProperties getGroupProperties(String groupId);

    /**
     * Update group's schema validation policy. 
     * 
     * @param groupId Id for the group. 
     * @param validationRules New compatibility setting for the group.
     */
    void updateGroupSchemaValidationRules(String groupId, SchemaValidationRules validationRules);
    
    /**
     * Gets list of object types registered under the group. Objects are identified by {@link SchemaInfo#name}
     * 
     * @param groupId Id for the group. 
     * @return List of different objects within the group.   
     */
    List<String> getGroupSchemaNames(String groupId);

    /**
     * Registers schema to the group. If group is configured with {@link GroupProperties#versionBySchemaName} then 
     * the {@link SchemaInfo#name} is used for validating against existing group schemas that share the same name. 
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to add. 
     *              
     * @return versionInfo which uniquely identifies where the schema is added in the group.   
     */
    VersionInfo addSchemaToGroup(String groupId, SchemaInfo schema);

    /**
     * Gets schema corresponding to the version. 
     * 
     * @param groupId Id for the group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return Schema info corresponding to the version info. 
     */
    SchemaInfo getGroupSchema(String groupId, VersionInfo version);

    /**
     * Gets encoding info against the requested encoding Id. 
     * Encoding Info uniquely identifies a combination of a schemaInfo and codecType. 
     * 
     * @param groupId Id for the group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return Encoding info corresponding to the encoding id. 
     */
    EncodingInfo getGroupEncodingInfo(String groupId, EncodingId encodingId);

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and codec type. 
     * 
     * @param groupId Id for the group. 
     * @param version version of schema 
     * @param codecType codec type
     * @return Encoding id for the pair of version and codec type.
     */
    EncodingId getGroupEncodingId(String groupId, VersionInfo version, CodecType codecType);

    /**
     * Gets latest schema and version for the group (or schemaName, if specified). 
     * For groups configured with {@link GroupProperties#versionBySchemaName}, the schemaName name needs to be supplied to 
     * get the latest schema for the schemaName. 
     * 
     * @param groupId Id for the group. 
     * @param schemaName Name of schemaName. 
     *                 
     * @return Schema with version for the last schema that was added to the group (or schemaName).
     */
    SchemaWithVersion getGroupLatestSchemaVersion(String groupId, @Nullable String schemaName);

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#versionBySchemaName}
     * the version will contain the name taken from the {@link SchemaInfo#name}. 
     * Version is uniquely identified by {@link SchemaInfo#schemaData}. 
     *
     * @param groupId Id for the group. 
     * @param schema SchemaInfo that captures schema name and schema data. 
     * @return VersionInfo corresponding to schema. 
     */
    VersionInfo getGroupVersionForSchema(String groupId, SchemaInfo schema);

    /**
     * Gets all schemas with corresponding versions for the group (or schemaName, if specified). 
     * For groups configured with {@link GroupProperties#versionBySchemaName}, the schemaName name needs to be supplied to 
     * get the latest schema for the schemaName. {@link SchemaInfo#name} is used as the schemaName name. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     * 
     * @param groupId Id for the group.
     * @param schemaName Name of schemaName. 
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    List<SchemaWithVersion> getGroupSchemaVersions(String groupId, @Nullable String schemaName);
    
    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * This api performs exactly the same validations as {@link RegistryClient#addSchemaToGroup(String, SchemaInfo)}
     * but without registering the schema. This is primarily to be used during schema development phase to validate that 
     * the changes to schema are in compliance with validation rules for the group.  
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to check for validity. 
     * @return True if it satisfies validation checks, false otherwise. 
     */
    boolean validateSchemaForGroup(String groupId, SchemaInfo schema);

    /**
     * Checks whether given schema can be used to read by validating it for reads against one or more existing schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to check to be used for reads. 
     * @return True if it can be used to read, false otherwise. 
     */
    boolean canReadGroupSchemasUsing(String groupId, SchemaInfo schema);

    /**
     * List of codecs used for encoding in the group. 
     * 
     * @param groupId Id for the group. 
     * @return List of codecs used for encoding in the group. 
     */
    List<CodecType> getGroupCodecTypes(String groupId);

    /**
     * Add new codec to be used in encoding in the group. 
     * 
     * @param groupId Id for the group. 
     * @param codecType codec type.
     */
    void addCodecTypeToGroup(String groupId, CodecType codecType);

    /**
     * Gets complete schema evolution history of the group with schemas, versions, rules and time for the group. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     *
     * @param groupId Id for the group.
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     */
    List<GroupHistoryRecord> getGroupHistory(String groupId);
}
