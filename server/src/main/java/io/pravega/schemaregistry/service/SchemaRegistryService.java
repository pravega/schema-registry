/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.util.concurrent.CompletableFuture;

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
     * @param scope
     * @return
     */
    CompletableFuture<Void> createScope(String scope);

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
     * @param scope
     * @param group
     * @return
     */
    CompletableFuture<List<String>> getSubgroups(String scope, String group);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param schema
     * @param rules
     * @return
     */
    CompletableFuture<VersionInfo> addSchemaIfAbsent(String scope, String group, @Nullable String subgroup, SchemaInfo schema,
                                                     SchemaValidationRules rules);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param version
     * @return
     */
    CompletableFuture<SchemaInfo> getSchema(String scope, String group, @Nullable String subgroup, VersionInfo version);

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
     * @param subgroup
     * @param version
     * @param compressionType
     * @return
     */
    CompletableFuture<EncodingId> getEncodingId(String scope, String group, @Nullable String subgroup, VersionInfo version, 
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
    CompletableFuture<VersionInfo> getSchemaVersion(String scope, String group, @Nullable String subgroup, SchemaInfo schema);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param writeVersion
     * @param readVersion
     * @return
     */
    CompletableFuture<Boolean> canRead(String scope, String group, @Nullable String subgroup, VersionInfo writeVersion, 
                                       VersionInfo readVersion);

    /**
     * 
     * @param scope
     * @param group
     * @param subgroup
     * @param schema
     * @return
     */
    CompletableFuture<Boolean> checkCompatibility(String scope, String group, @Nullable String subgroup, SchemaInfo schema);
}
