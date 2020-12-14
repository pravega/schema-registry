/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (String namespace, the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Schema Store interface for storing and retrieving and querying schemas. 
 */
public interface SchemaStore {
    /**
     * Fetch Groups from the store starting from supplied Continuation token and upto the specified limit.
     * There is no guarantee about atomic retrieval of groups from the store. The implementation will retrieve at most "limit"
     * number of groups but it could be smaller than that. 
     * There should be a consistent order in which groups are returned from the continuation token.
     * 
     * @param namespace namespace in which to fetch groups from. 
     * @param token continuation token.
     * @param limit number of elements to retrieve.
     * @return Completablefuture that holds List with continuation token 
     */
    CompletableFuture<ResultPage<String, ContinuationToken>> listGroups(String namespace, @Nullable ContinuationToken token, int limit);

    /**
     * Create a new group within the namespace. It implicitly creates the namespace if it isnt already created.
     * Creating a group is idempotent. If a group by the name already exists, the api returns false if its properties 
     * are different from current request, true otherwise. 
     * The implementation should guarantee that group creation is performed with a concurrency guarantee such that
     * no operation is allowed on a group until it is created. However, multiple concurrent create group requests can 
     * be attempted and the implementation should guarantee atomic creation of group's metadata. 
     * 
     * Note that the namespace is an attribute of the group and there are no explicit apis to perform CRUD operations on namespaces.
     * 
     * @param namespace namespace
     * @param group group 
     * @param groupProperties group properties to use for the group.
     * @return Completablefuture that holds True if the group was created afresh, false if the group exists. 
     */
    CompletableFuture<Boolean> createGroup(String namespace, String group, GroupProperties groupProperties);

    /**
     * Deletes a group identified by the namespace and group. This first deletes the group metadata followed by removing the
     * group entry from groups table. Deleting a group is idempotent. A group can only be deleted if it is already in Active state. 
     * Deleting a group is non atomic.
     * @param namespace namespace
     * @param group group
     * @return Completable Future which completes successfully if the group is deleted.  
     */
    CompletableFuture<Void> deleteGroup(String namespace, String group);

    /**
     * Gets an entity tag for the group. All update operations on the group metadata are performed with the entity tag. 
     * So all metadata operations on group perform compare and set on the metadata and entity tag and if multiple concurrent
     * updates to group metadata are attempted, they could fail with Write Conflict. 
     * 
     * @param namespace namespace
     * @param group group
     * @return Completablefuture that holds Entity tag for the group. 
     */
    CompletableFuture<Etag> getGroupEtag(String namespace, String group);

    /**
     * Gets the group properties metadata for the group. This should be an atomic operation. 
     * 
     * @param namespace namespace 
     * @param group group
     * @return Completable Future which will hold group properties upon completion. 
     */
    CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group);

    /**
     * Updates the compatibility policy for the group conditionally (etag). This should be updated atomically. 
     * 
     * @param namespace namespace
     * @param group group
     * @param etag entity tag
     * @param policy new policy 
     * @return Completable future which is completed successfully if the update completes. 
     */
    CompletableFuture<Void> updateCompatibility(String namespace, String group, Etag etag, Compatibility policy);

    /**
     * Gets latest schemas for all types in the group. The implementation for this API should fetch all latest schemas 
     * across all types atomically. There is no order on the schemas in the list. 
     * 
     * @param namespace namespace
     * @param group group
     * @return Completablefuture that holds List of latest schemas with versions for all types in the group. 
     */
    CompletableFuture<List<SchemaWithVersion>> listLatestSchemas(String namespace, String group);

    /**
     * Gets all schemas for all types in the group. The implementation for this API should fetch all schemas 
     * across atomically. The schema id {@link VersionInfo#id} defines the order in which the schemas were added and the list
     *      * is ordered by the schema id. 
     *
     * @param namespace namespace
     * @param group group
     * @return Completablefuture that holds List of latest schemas with versions for all types in the group. 
     */
    CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String group);

    /**
     * Gets all schemas for all types in the group that were added after the specified version. 
     * The implementation for this API should fetch all schemas atomically.
     * The schema id {@link VersionInfo#id} defines the order in which the schemas were added and the list
     * is ordered by the schema id. 
     *
     * @param namespace namespace
     * @param group group
     * @param from version from which to fetch the schemas. 
     * @return Completablefuture that holds List of latest schemas with versions for all types in the group. 
     */
    CompletableFuture<List<SchemaWithVersion>> listSchemas(String namespace, String group, VersionInfo from);

    /**
     * Gets all schemas for specified type in the group. 
     * The implementation for this API should fetch all schemas atomically.
     * The schema id {@link VersionInfo#id} defines the order in which the schemas were added and the list
     * is ordered by the schema id. 
     *
     * @param namespace namespace
     * @param group group
     * @param schemaType type of schemas to fetch. 
     * @return Completablefuture that holds List of latest schemas with versions for specified type in the group. 
     */
    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String group, String schemaType);

    /**
     * Gets all schemas for specified type in the group. 
     * The implementation for this API should fetch all schemas atomically.
     * The schema id {@link VersionInfo#id} defines the order in which the schemas were added and the list
     * is ordered by the schema id. 
     *
     * @param namespace namespace
     * @param group group
     * @param schemaType type of schemas to fetch.
     * @param from version from which to fetch the schemas. 
     * @return Completablefuture that holds List of latest schemas with versions for the specified in the group. 
     */
    CompletableFuture<List<SchemaWithVersion>> listSchemasByType(String namespace, String group, String schemaType, VersionInfo from);

    /**
     * Deletes the schema identified by schema id. This should perform soft delete schema. The implementation should guarantee atomic soft deletion. 
     * Delete should be idempotent. 
     * 
     * @param namespace namespace
     * @param group group
     * @param schemaId schema identifier 
     * @param etag entity tag for the group. 
     * @return Completable future that is completed successfully if the schema is deleted
     */
    CompletableFuture<Void> deleteSchema(String namespace, String group, int schemaId, Etag etag);

    /**
     * Deletes the schema identified by type and verison. 
     * This should perform soft delete schema. The implementation should guarantee atomic soft deletion. 
     * Delete should be idempotent. 
     *
     * @param namespace namespace
     * @param group group
     * @param serializationFormat serialization format
     * @param schemaType schema type
     * @param version schema version
     * @param etag entity tag for the group. 
     * @return Completable future that is completed successfully if the schema is deleted
     */
    CompletableFuture<Void> deleteSchema(String namespace, String group, String schemaType, int version, String serializationFormat, Etag etag);

    /**
     * Get the schema corresponding to the schema id. 
     * 
     * @param namespace namespace 
     * @param group group 
     * @param schemaId schema id
     * @return Completablefuture that holds schemainfo for the schema identified by the specified id. 
     */
    CompletableFuture<SchemaInfo> getSchema(String namespace, String group, int schemaId);

    /**
     * Get the schema corresponding to the schema identified by the type and version. 
     *
     * @param namespace namespace 
     * @param group group 
     * @param serializationFormat serialization format
     * @param schemaType schema type
     * @param version schema version
     * @return Completablefuture that holds schemainfo for the schema identified by the specified type and verison. 
     */
    CompletableFuture<SchemaInfo> getSchema(String namespace, String group, String schemaType, int version, String serializationFormat);

    /**
     * Get the latest schema version for the group across all types. This is the last schema added to the group.  
     *
     * @param namespace namespace 
     * @param group group 
     * @return Completablefuture that holds schemainfo for the last schema added to the group. 
     */
    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String group);

    /**
     * Get the latest schema version for the group for specific type. This is the last schema added to the group for the type.  
     *
     * @param namespace namespace 
     * @param group group 
     * @param type schema type
     * @return Completablefuture that holds schemainfo for the last schema for the type added to the group. 
     */
    CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String namespace, String group, String type);

    /**
     * Add a new schema to the group atomically. this should generate a new version number and id for the schema being added.
     * Version is monotonically increasing number for the schema type. And id is the monotonically increasing number for
     * the schemas added to the group. 
     *
     * @param namespace namespace 
     * @param group group 
     * @param schemaInfo schema to add
     * @param normalized normalized form of schema to add.
     * @param fingerprint 256 bit sha hash of schema binary. 
     *                    Two schema binary representation will be considered identical if their fingerprints match.
     * @param prop group properties applied at the time of schema addition.
     * @param etag entity tag for the group. 
     * @return Completablefuture that holds version info for the schema that is added.  
     */
    CompletableFuture<VersionInfo> addSchema(String namespace, String group, SchemaInfo schemaInfo, SchemaInfo normalized,
                                             BigInteger fingerprint, GroupProperties prop, Etag etag);

    /**
     * Get the version corresponding to the schema.  
     *
     * @param namespace namespace 
     * @param group group 
     * @param schemaInfo schemainfo
     * @param fingerprint 256 bit sha hash of schema binary. 
     *                    Two schema binary representation will be considered identical if their fingerprints match.
     * @return Completablefuture that holds versioninfo for the schema. 
     */
    CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schemaInfo, BigInteger fingerprint);

    /**
     * Get the encoding id corresponding to versioninfo and codectype. It returns Etag for the group if the encoding id
     * does not exist for the given pair. 
     *
     * @param namespace namespace 
     * @param group group 
     * @param versionInfo versioninfo
     * @param codecType codectype
     * @return Completablefuture that holds encodingId. If the encoding id doesnt exist it returns the entity tag identifying
     * the entity state when the encoding id did not exist
     */
    CompletableFuture<Either<EncodingId, Etag>> getEncodingId(String namespace, String group, VersionInfo versionInfo, String codecType);

    /**
     * Create a new encoding id for the pair atomically.  
     *
     * @param namespace namespace 
     * @param group group 
     * @param versionInfo versioninfo
     * @param codecType codectype
     * @param etag entity tag for the group. 
     * @return Completablefuture that holds encodingId. 
     */
    CompletableFuture<EncodingId> createEncodingId(String namespace, String group, VersionInfo versionInfo, String codecType, Etag etag);

    /**
     * Get encoding id corresponding to the encoding id.    
     * @param namespace namespace 
     * @param group group 
     * @param encodingId encoding id
     * @return CompletableFuture that holds the encoding info. 
     */
    CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId);

    /**
     * Gets list of codec types added to the group atomically. 
     *
     * @param namespace namespace 
     * @param group group 
     * @return CompletableFuture that holds list of codec types.  
     */
    CompletableFuture<List<CodecType>> listCodecTypes(String namespace, String group);

    /**
     * Add a new codectype to the group. This api is idempotent. 
     *
     * @param namespace namespace 
     * @param group group
     * @param codecType codectype to add
     * @return CompletableFuture that is completed successfully if the codec is added.  
     */
    CompletableFuture<Void> addCodecType(String namespace, String group, CodecType codecType);

    /**
     * Gets the schema evolution history of the group with respect to schema additions. the history of the group is ordered by
     * the order in which schemas were added. It includes schemas for all types. 
     *
     * @param namespace namespace 
     * @param group group 
     * @return CompletableFuture that holds list of group history record.  
     */
    CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String namespace, String group);

    /**
     * Gets the schema evolution history of the group with respect to schema additions for the specific type.
     * the history of the group is ordered by the order in which schemas were added. 
     *
     * @param namespace namespace 
     * @param group group 
     * @param type  type of schema 
     * @return CompletableFuture that holds list of group history record.  
     */
    CompletableFuture<List<GroupHistoryRecord>> getGroupHistoryForType(String namespace, String group, String type);

    /**
     * Gets list of groups in the given namespace that use the specified schema. 
     * 
     * @param namespace namespace.
     * @param schemaInfo Schema being referenced. 
     * @return CompletableFuture that holds a List of group ids where the schema may have been added. The group id is 
     * included even if the schema addition was deleted. 
     */
    CompletableFuture<List<String>> getGroupsUsing(String namespace, SchemaInfo schemaInfo);
}
