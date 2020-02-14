/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionType;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionsList;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingId;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingInfo;
import io.pravega.schemaregistry.server.rest.generated.model.GroupProperty;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaEvolutionList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaType;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersion;
import io.pravega.schemaregistry.server.rest.generated.model.Compatibility;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaValidationRules;
import io.pravega.schemaregistry.server.rest.generated.model.VersionInfo;

import java.util.Base64;
import java.util.List;

public class ModelHelper {

    // region decode
    public static SchemaRegistryContract.SchemaInfo decode(SchemaInfo schemaInfo) {
        SchemaRegistryContract.SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new SchemaRegistryContract.SchemaInfo(schemaInfo.getSchemaName(), schemaType, Base64.getEncoder().encodeToString(schemaInfo.getSchemaData()), 
                ImmutableMap.copyOf(schemaInfo.getProperties()));
    }

    public static SchemaRegistryContract.SchemaType decode(SchemaType schemaType) {
        return null;
    }

    public static SchemaValidationRules decode(io.pravega.schemaregistry.server.rest.generated.model.SchemaValidationRules rules) {
        return null;
    }

    public static io.pravega.schemaregistry.contract.Compatibility decode(Compatibility compatibility) {
        return null;
    }

    public static SchemaRegistryContract.CompressionType decode(CompressionType compressionType) {
        return null;
    }

    public static SchemaRegistryContract.VersionInfo decode(VersionInfo versionInfo) {
        return null;
    }
    // endregion
    
    // region encode
    public static SchemaEvolutionList encode(List<SchemaRegistryContract.SchemaEvolution> schemasWithVersions) {
        return null;
    }

    public static SchemaWithVersion encode(SchemaRegistryContract.SchemaWithVersion schemaWithVersion) {
        return null;
    }

    public static GroupProperty encode(SchemaRegistryContract.GroupProperties groupProperties) {
        return null;
    }

    public static GroupProperty encode(String groupName, SchemaRegistryContract.GroupProperties groupProperties) {
        return ModelHelper.encode(groupProperties).groupName(groupName);
    }

    public static VersionInfo encode(SchemaRegistryContract.VersionInfo versionInfo) {
        return null;
    }
    
    public static SchemaInfo encode(SchemaRegistryContract.SchemaInfo schemaWithVersion) {
        return null;
    }
    
    public static EncodingId encode(SchemaRegistryContract.EncodingId encodingInfo) {
        return null;
    }

    public static EncodingInfo encode(SchemaRegistryContract.EncodingInfo encodingInfo) {
        return null;
    }

    public static CompressionsList encodeCompressionList(List<SchemaRegistryContract.CompressionType> list) {
        return null;
    }
    // endregion
}
