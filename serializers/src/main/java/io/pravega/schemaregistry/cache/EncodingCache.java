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
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import lombok.SneakyThrows;
import lombok.Synchronized;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Local cache for storing schemas that are retrieved from the registry service.  
 */
public class EncodingCache {
    private static final ConcurrentHashMap<String, EncodingCache> GROUP_CACHE_MAP = new ConcurrentHashMap<>();
    
    private final LoadingCache<EncodingId, EncodingInfo> encodingCache;
    
    private EncodingCache(String groupId, SchemaRegistryClient registryClient) {
        encodingCache = CacheBuilder.newBuilder().build(new CacheLoader<EncodingId, EncodingInfo>() {
            @Override
            public EncodingInfo load(EncodingId key) {
                return registryClient.getEncodingInfo(groupId, key);
            }
        });
    }
    
    @Synchronized
    public static EncodingCache getEncodingCacheForGroup(String groupId, SchemaRegistryClient registryClient) {
        if (GROUP_CACHE_MAP.containsKey(groupId)) {
            return GROUP_CACHE_MAP.get(groupId);
        } else {
            EncodingCache value = new EncodingCache(groupId, registryClient);
            GROUP_CACHE_MAP.put(groupId, value);
            return value;
        }
    }
    
    @SneakyThrows(ExecutionException.class)
    public EncodingInfo getEncodingInfo(EncodingId encodingId) {
        return encodingCache.get(encodingId);
    }
}
