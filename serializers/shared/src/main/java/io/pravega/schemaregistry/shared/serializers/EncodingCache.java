/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.shared.serializers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Local cache for storing schemas that are retrieved from the registry service.  
 */
public class EncodingCache {
    private static final int MAXIMUM_SIZE = 1000;
    
    private final LoadingCache<EncodingId, EncodingInfo> encodingCache;
    public EncodingCache(String groupId, SchemaRegistryClient schemaRegistryClient) {
        this(groupId, schemaRegistryClient, MAXIMUM_SIZE);
    }

    @VisibleForTesting
    EncodingCache(String groupId, SchemaRegistryClient schemaRegistryClient, int cacheSize) {
        encodingCache = CacheBuilder.newBuilder()
                                    .maximumSize(cacheSize)
                                    .build(new CacheLoader<EncodingId, EncodingInfo>() {
            @Override
            public EncodingInfo load(EncodingId key) {
                return schemaRegistryClient.getEncodingInfo(groupId, key);
            }
        });
    }
    
    EncodingInfo getGroupEncodingInfo(EncodingId encodingId) {
        try {
            return encodingCache.get(encodingId);
        } catch (ExecutionException e) {
            if (e.getCause() != null && Exceptions.unwrap(e.getCause()) instanceof RegistryExceptions) {
                throw (RegistryExceptions) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }
    
    @VisibleForTesting
    ConcurrentMap<EncodingId, EncodingInfo> getMapForCache() {
        return encodingCache.asMap();
    }
}
