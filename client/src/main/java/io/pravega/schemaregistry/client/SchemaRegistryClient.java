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

import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static io.pravega.schemaregistry.client.exceptions.RegistryExceptions.*;

/**
 * Defines a registry client for interacting with schema registry service. 
 */
public interface SchemaRegistryClient {
    /**
     * Adds a new group. A group refers to the name under which the schemas are registered. A group is identified by a 
     * unique name and has an associated set of group metadata {@link GroupProperties} and a list of codec types and a 
     * versioned history of schemas that were registered under the group. 
     * Add group is idempotent. If the group by the same name already exists the api will return false. 
     * 
     * @param groupId Id for the group that uniquely identifies the group. 
     * @param groupProperties groupProperties Group properties for the group. These include serialization format, validation rules, 
     *                        and flag to declare whether multiple schemas representing distinct object types can be 
     *                        registered with the group. Type identify objects of same type. Schema compatibility checks 
     *                        are always performed for schemas that share same {@link SchemaInfo#type}.
     *                        Additionally, a user defined map of properties can be supplied.
     * @return True indicates if the group was added successfully, false if it exists. 
     * @throws BadArgumentException if the group properties is rejected by service.
     * @throws UnauthorizedException if the user is unauthorized.
     */
    boolean addGroup(String groupId, GroupProperties groupProperties) throws BadArgumentException, UnauthorizedException;
    
    /**
     * Removes a group identified by the groupId. This will remove all the codec types and schemas registered under the group.
     * Remove group is idempotent. 
     * 
     * @param groupId Id for the group that uniquely identifies the group. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    void removeGroup(String groupId) throws UnauthorizedException;

    /**
     * List all groups. The returned value contains a Map of group name to group properties. 
     * For partially created/failed groups the group properties value will be null. 
     * The processing of this api is atomic and if groups are added after the api completes, then those will 
     * not be included in the response. 
     * 
     * @return map of names of groups with corresponding group properties for all groups. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    Map<String, GroupProperties> listGroups() throws UnauthorizedException;
    
    /**
     * Get group properties for the group identified by the group id. 
     * 
     * {@link GroupProperties#serializationFormat} which identifies the serialization format is used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#allowMultipleTypes} that specifies if multiple schemas are allowed to be registered in the group. 
     * Schemas are validated against existing schema versions that have the same {@link SchemaInfo#type}. 
     * {@link GroupProperties#properties} describes generic properties for a group.
     * 
     * @param groupId Id for the group.
     * @return Group properties which includes property like Serialization format and compatibility policy. 
     * @throws ResourceNotFoundException if group is not found.
     * @throws UnauthorizedException if the user is unauthorized.
     */
    GroupProperties getGroupProperties(String groupId) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Update group's schema validation policy. If previous rules are not supplied, then the update to the rules will be
     * performed unconditionally. However, if previous rules are supplied, then the update will be performed if and only if
     * existing {@link GroupProperties#schemaValidationRules} match previous rules. 
     * 
     * @param groupId Id for the group. 
     * @param validationRules New Schema validation rules for the group.
     * @param previousRules Previous schema validation rules.
     * @throws PreconditionFailedException if previous rules do not match the rules set for the group.
     * @throws ResourceNotFoundException if group is not found.
     * @throws UnauthorizedException if the user is unauthorized.
     */
    void updateSchemaValidationRules(String groupId, SchemaValidationRules validationRules, @Nullable SchemaValidationRules previousRules)
        throws PreconditionFailedException, ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets list of latest schemas for each object types registered under the group. Objects are identified by {@link SchemaInfo#type}.
     *
     * @param groupId Id for the group. 
     * @return List of different objects within the group.   
     * @throws ResourceNotFoundException if group is not found.
     * @throws UnauthorizedException if the user is unauthorized.
     */
    List<SchemaWithVersion> getSchemas(String groupId) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Registers schema to the group. Schemas are validated against existing schemas in the group that share the same 
     * {@link SchemaInfo#type}.
     * If group is configured with {@link GroupProperties#allowMultipleTypes} then multiple schemas with distinct
     * type {@link SchemaInfo#type} could be registered. 
     * All schemas with same type are assigned monotonically increasing version numbers. 
     * Add schema api is idempotent. If a schema is already registered, its version info is returned by the service.  
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to add. 
     * @return versionInfo which uniquely identifies where the schema is added in the group. If schema is already registered,
     * then the existing version info is returned. 
     * @throws SchemaValidationFailedException if the schema is deemed invalid by applying schema validation rules which may 
     * include comparing schema with existing schemas for compatibility in the desired direction. 
     * @throws SerializationMismatchException if serialization format does not match the group's configured serialization format.
     * @throws MalformedSchemaException for known serialization formats, if the service is unable to parse the schema binary or 
     * for avro and protobuf if the {@link SchemaInfo#type} does not match the name of record/message in the binary.
     * @throws ResourceNotFoundException if group is not found.
     * @throws UnauthorizedException if the user is unauthorized.
     */
    VersionInfo addSchema(String groupId, SchemaInfo schema) throws SchemaValidationFailedException, SerializationMismatchException, 
            MalformedSchemaException, ResourceNotFoundException, UnauthorizedException;

    /**
     * Api to delete schema corresponding to the version. Users should be very careful while using this API and it is 
     * advised to not be used in production, esp if the schema has already been used to write the data. 
     * Delete schema api is idempotent. 
     * This does a soft delete of the schema. So getSchemaVersion with the version info will still return the schema.
     * However, the schema will not participate in any compatibility checks once deleted.
     * It will not be included in listing schema versions for the group using apis like {@link SchemaRegistryClient#getSchemaVersions}
     * or {@link SchemaRegistryClient#getGroupHistory} or {@link SchemaRegistryClient#getSchemas} or 
     * {@link SchemaRegistryClient#getLatestSchemaVersion}
     * If add schema is called again using this deleted schema will result in a new version being assigned to it subject to 
     * schema validation rules. 
     * 
     * @param groupId Id for the group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    void deleteSchemaVersion(String groupId, VersionInfo version) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets schema corresponding to the version. 
     * 
     * @param groupId Id for the group. 
     * @param version Version which uniquely identifies schema within a group. 
     * @return Schema info corresponding to the version info.
     * @throws ResourceNotFoundException if group or version is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    SchemaInfo getSchemaForVersion(String groupId, VersionInfo version) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets encoding info against the requested encoding Id. The purpose of encoding info is to uniquely identify the encoding
     * used on the data at rest. The encoding covers two parts - 
     * 1. Schema that defines the structure of the data and is used for serialization. A specific schema version registered with
     * registry service is uniquely identified by the corresponding VersionInfo object. 
     * 2. CodecType that is used to encode the serialized data. This would typically be some compression. The codecType 
     * and schema should both be registered with the service and service will generate a unique identifier for each such 
     * pair. 
     * Encoding Info uniquely identifies a combination of a versionInfo and codecType.
     * EncodingInfo also includes the {@link SchemaInfo} identified by the {@link VersionInfo}.
     * 
     * @param groupId Id for the group. 
     * @param encodingId Encoding id that uniquely identifies a schema within a group. 
     * @return Encoding info corresponding to the encoding id. 
     * @throws ResourceNotFoundException if group or encoding id is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    EncodingInfo getEncodingInfo(String groupId, EncodingId encodingId) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and codec type. 
     * This encoding id is a 4 byte integer and it can be used to tag the data which is serialized and encoded using the
     * schema version and codecType identified by this encoding id. 
     * This api is idempotent. And if an encoding id is generated for a version and codec pair, subsequent requests to this
     * api will return the generated encoding id. 
     * If the schema identified by the version is deleted using {@link SchemaRegistryClient#deleteSchemaVersion} api, 
     * then if the encoding id was already generated for the pair of schema version and codec, then it will be returned. 
     * However, if no encoding id for the versioninfo and codec pair was generated and the schema version was deleted, 
     * then any call to getEncodingId using the deleted versionInfo will throw ResourceNotFoundException. 
     * 
     * @param groupId Id for the group. 
     * @param version version of schema 
     * @param codecType codec type
     * @return Encoding id for the pair of version and codec type.
     * @throws CodecTypeNotRegisteredException if codectype is not registered with the group. Use {@link SchemaRegistryClient#addCodecType} 
     * @throws ResourceNotFoundException if group or version info is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    EncodingId getEncodingId(String groupId, VersionInfo version, String codecType) 
            throws CodecTypeNotRegisteredException, ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets latest schema and version for the group (or type, if specified). 
     * To get latest schema version for a specific type identified by {@link SchemaInfo#type}, provide the type. 
     * Otherwise if the group is configured to allow multiple schemas {@link GroupProperties#allowMultipleTypes}, then 
     * and type is not specified, then last schema added to the group across all types will be returned. 
     * 
     * @param groupId Id for the group. 
     * @param schemaType Type of object identified by {@link SchemaInfo#type}. 
     *                 
     * @return Schema with version for the last schema that was added to the group (or type).
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    SchemaWithVersion getLatestSchemaVersion(String groupId, @Nullable String schemaType)
        throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets version corresponding to the schema.  
     * For each schema type {@link SchemaInfo#type} and {@link SchemaInfo#serializationFormat} a versionInfo object uniquely 
     * identifies each distinct {@link SchemaInfo#schemaData}. 
     *
     * @param groupId Id for the group. 
     * @param schema SchemaInfo that describes format and structure. 
     * @return VersionInfo corresponding to schema. 
     * @throws ResourceNotFoundException if group is not found or if schema is not registered. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    VersionInfo getVersionForSchema(String groupId, SchemaInfo schema) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets all schemas with corresponding versions for the group (or type, if specified). 
     * For groups configured with {@link GroupProperties#allowMultipleTypes}, the type {@link SchemaInfo#type} should be 
     * supplied to view schemas specific to a type. if type is null, all schemas in the group are returned.  
     * The order in the list matches the order in which schemas were evolved within the group. 
     * 
     * @param groupId Id for the group.
     * @param schemaType type of object identified by {@link SchemaInfo#type}. 
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    List<SchemaWithVersion> getSchemaVersions(String groupId, @Nullable String schemaType) throws ResourceNotFoundException, UnauthorizedException;
    
    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * This api performs exactly the same validations as {@link SchemaRegistryClient#addSchema(String, SchemaInfo)}
     * but without registering the schema. This is primarily intended to be used during schema development phase to validate that 
     * the changes to schema are in compliance with validation rules for the group.  
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to check for validity. 
     * @return A schema is valid if it passes all the {@link GroupProperties#schemaValidationRules}. The rule supported 
     * presently, is Compatibility. If desired compatibility is satisfied by the schema then this api returns true, false otherwise. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    boolean validateSchema(String groupId, SchemaInfo schema) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Checks whether given schema can be used to read by validating it for reads against one or more existing schemas in the group  
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * 
     * @param groupId Id for the group. 
     * @param schema Schema to check to be used for reads. 
     * @return True if it can be used to read, false otherwise. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    boolean canReadUsing(String groupId, SchemaInfo schema) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * List of codec types used for encoding in the group. 
     * 
     * @param groupId Id for the group. 
     * @return List of codec types used for encoding in the group. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    List<String> getCodecTypes(String groupId) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Add new codec type to be used in encoding in the group. 
     * 
     * @param groupId Id for the group. 
     * @param codecType codec type.
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    void addCodecType(String groupId, String codecType) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Gets complete schema evolution history of the group with schemas, versions, rules and time for the group. 
     * The order in the list matches the order in which schemas were evolved within the group. 
     *
     * @param groupId Id for the group.
     * @return Ordered list of schemas with versions and validation rules for all schemas in the group. 
     * @throws ResourceNotFoundException if group is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    List<GroupHistoryRecord> getGroupHistory(String groupId) throws ResourceNotFoundException, UnauthorizedException;

    /**
     * Finds all groups and corresponding version info for the groups where the supplied schema has been registered.
     * It is important to note that the same schema type could be part of multiple group, however in each group it 
     * may have gone through a separate evolution. This api simply identifies all groups where the specific schema 
     * (type, format and binary) is used. 
     * The user defined {@link SchemaInfo#properties} is not used for comparison. 
     * 
     * @param schemaInfo Schema info to find references for. 
     * @return Map of group Id to versionInfo identifier for the schema in that group. 
     * @throws ResourceNotFoundException if schema is not found. 
     * @throws UnauthorizedException if the user is unauthorized.
     */
    Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws ResourceNotFoundException, UnauthorizedException;
}
