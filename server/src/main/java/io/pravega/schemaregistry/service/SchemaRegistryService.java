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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.pravega.schemaregistry.contract.Compatibility;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingId;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.GroupProperties;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaWithVersion;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaValidationRules;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.VersionInfo;

public interface SchemaRegistryService {
    /**
     *
     * @return
     */
    CompletableFuture<List<String>> listScopes();

    /**
     * 
     * @param scope
     * @return
     */
    CompletableFuture<Void> createScope(String scope);

    /**
     * 
     * @param scope
     * @return
     */
    CompletableFuture<Map<String, GroupProperties>> listGroupsInScope(String scope);

    /**
     * 
     * @param scope
     * @param group
     * @param groupProperties
     * @return
     */
    CompletableFuture<Boolean> createGroup(String scope, String group, GroupProperties groupProperties);

    /**
     * 
     * @param scope
     * @param group
     * @return
     */
    CompletableFuture<GroupProperties> getGroupProperties(String scope, String group);

    /**
     *
     * @param scopeName
     * @param groupName
     * @param compatibility
     * @return
     */
    CompletableFuture<Void> updateCompatibilityPolicy(String scopeName, String groupName, Compatibility compatibility);

    /**
     * 
     * @param scope
     * @param group
     * @return
     */
    CompletableFuture<List<String>> getSubgroups(String scope, String group);

    /**
     * 
     * @param scope
     * @param group
     * @param schema
     * @param rules
     * @return
     */
    CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String group, SchemaInfo schema,
                                                     SchemaValidationRules rules);

    /**
     * 
     * @param scope
     * @param group
     * @param version
     * @return
     */
    CompletableFuture<SchemaInfo> getSchema(String scope, String group, VersionInfo version);

    /**
     * 
     * @param scope
     * @param group
     * @param encodingId
     * @return
     */
    CompletableFuture<EncodingInfo> getEncodingInfo(String scope, String group, EncodingId encodingId);

    /**
     * 
     * @param scope
     * @param group
     * @param version
     * @param compressionType
     * @return
     */
    CompletableFuture<EncodingId> getEncodingId(String scope, String group, VersionInfo version, 
                                                CompressionType compressionType);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @return
     */
    CompletableFuture<SchemaWithVersion> getLatestSchema(String scope, String group, @Nullable String subgroup);

    /**
     * 
     * @param scope
     * @param groupName
     * @param subgroup
     * @return
     */
    CompletableFuture<List<SchemaWithVersion>> getAllSchemas(String scope, String groupName, @Nullable String subgroup);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param schema
     * @return
     */
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, SchemaInfo schema);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param writeVersion
     * @param readVersion
     * @return
     */
    CompletableFuture<Boolean> canRead(String scope, String group, VersionInfo writeVersion, VersionInfo readVersion);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param schema
     * @return
     */
    CompletableFuture<Boolean> checkCompatibility(String scope, String group, SchemaInfo schema);

    /**
     * 
     * @param scopeName
     * @param groupName
     * @return
     */
    CompletableFuture<Void> deleteGroup(String scopeName, String groupName);

    CompletableFuture<List<CompressionType>> getCompressions(String scope, String group);
}
