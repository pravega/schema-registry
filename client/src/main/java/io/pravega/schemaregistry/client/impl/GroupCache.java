/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingId;
import lombok.SneakyThrows;

import java.util.concurrent.ExecutionException;

class GroupCache {
    private final String scope;
    private final String groupId;
    private final SchemaRegistryClient registryClient;

    private final Cache<SchemaRegistryContract.EncodingInfo, EncodingId> versionCache;
    private final Cache<EncodingId, SchemaRegistryContract.EncodingInfo> schemaCache;

    GroupCache(String scope, String groupId, SchemaRegistryClient registryClient) {
        this.groupId = groupId;
        this.scope = scope;
        this.registryClient = registryClient;
        schemaCache = CacheBuilder.newBuilder().build();

        versionCache = CacheBuilder.newBuilder().build();
    }

    @SneakyThrows(ExecutionException.class)
    EncodingId getEncodingId(SchemaRegistryContract.SchemaInfo schemaInfo, CompressionType compressionType) {
        return versionCache.get(new SchemaRegistryContract.EncodingInfo(schemaInfo, compressionType),
                () -> {
                    SchemaRegistryContract.VersionInfo versionInfo = registryClient.getSchemaVersion(scope, groupId, schemaInfo);
                    return registryClient.getEncodingId(scope, groupId, versionInfo, compressionType);
                });
    }

    @SneakyThrows(ExecutionException.class)
    SchemaRegistryContract.EncodingInfo getSchemaFromEncoding(EncodingId encodingId) {
        return schemaCache.get(encodingId,
                () -> registryClient.getEncodingInfo(scope, groupId, encodingId));
    }
}
