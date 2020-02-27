/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.SneakyThrows;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.ExecutionException;

public class EncodingCache {
    private final LoadingCache<SchemaInfo, VersionInfo> schemaCache;
    private final LoadingCache<EncodingInfo, EncodingId> versionCache;
    private final LoadingCache<EncodingId, EncodingInfo> encodingCache;

    public EncodingCache(String scope, String stream, SchemaRegistryClient registryClient) {
        schemaCache = CacheBuilder.newBuilder().build(new CacheLoader<SchemaInfo, VersionInfo>() {
                                                      @ParametersAreNonnullByDefault
                                                      @Override
                                                      public VersionInfo load(final SchemaInfo key) {
                                                          return registryClient.getSchemaVersion(scope, stream, key);
                                                      }
                                                  });
        versionCache = CacheBuilder.newBuilder().build(new CacheLoader<EncodingInfo, EncodingId>() {
                                                      @ParametersAreNonnullByDefault
                                                      @Override
                                                      public EncodingId load(final EncodingInfo key) {
                                                          return registryClient.getEncodingId(scope, stream, key.getVersionInfo(), 
                                                                  key.getCompression());
                                                      }
                                                  });

        encodingCache = CacheBuilder.newBuilder().build(new CacheLoader<EncodingId, EncodingInfo>() {
            @Override
            public EncodingInfo load(EncodingId key) throws Exception {
                return registryClient.getEncodingInfo(scope, stream, key);
            }
        });
        
    }

    @SneakyThrows(ExecutionException.class)
    public VersionInfo getVersionFromSchema(SchemaInfo schemaInfo) {
        return schemaCache.get(schemaInfo);
    }

    @SneakyThrows(ExecutionException.class)
    public EncodingId getEncodingId(SchemaInfo schemaInfo, CompressionType compressionType) {
        VersionInfo versionInfo = getVersionFromSchema(schemaInfo);
        return versionCache.get(new EncodingInfo(versionInfo, schemaInfo, compressionType));
    }

    @SneakyThrows(ExecutionException.class)
    public EncodingInfo getEncodingInfo(EncodingId encodingId) {
        return encodingCache.get(encodingId);
    }
}
