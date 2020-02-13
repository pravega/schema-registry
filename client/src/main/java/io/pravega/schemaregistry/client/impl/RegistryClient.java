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
import io.pravega.schemaregistry.contract.SchemaRegistryContract.VersionInfo;
import io.pravega.schemaregistry.contract.SchemaValidationRules;

import javax.annotation.Nullable;
import java.util.List;

public interface RegistryClient {
    boolean addGroup(String scope, String groupId, SchemaType type, Compatibility compatibility, boolean allowSubgroups, boolean encodeHeader);
    
    GroupProperties getGroupProperties(String scope, String groupId);
    
    void updateCompatibilityPolicy(String scope, String groupId, Compatibility compatibility);
    
    List<String> getSubgroups(String scope, String groupId);

    VersionInfo addSchemaIfAbsent(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema,
                                  SchemaValidationRules rules);
    
    SchemaInfo getSchema(String scope, String groupId, @Nullable String subgroupId, VersionInfo version);
    
    EncodingInfo getEncodingInfo(String scope, String groupId, EncodingId encodingId);

    EncodingId getEncodingId(String scope, String groupId, @Nullable String subgroupId, VersionInfo version, CompressionType compressionType);
    
    SchemaWithVersion getLatestSchema(String scope, String groupId, @Nullable String subgroupId);

    List<SchemaWithVersion> getAllSchemas(String scope, String groupName, @Nullable String subgroupId);

    VersionInfo getSchemaVersion(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema);

    boolean canRead(String scope, String groupId, @Nullable String subgroupId, VersionInfo writeVersion, VersionInfo readVersion);

    boolean checkCompatibility(String scope, String groupId, @Nullable String subgroupId, SchemaInfo schema);
}
