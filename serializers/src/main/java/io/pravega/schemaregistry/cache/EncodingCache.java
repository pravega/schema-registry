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
import lombok.Data;
import lombok.SneakyThrows;
import lombok.Synchronized;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Local cache for storing schemas that are retrieved from the registry service.  
 */
public class EncodingCache {
    private static final Map<Key, EncodingCache> GROUP_CACHE_MAP = new HashMap<>();
    
    private final LoadingCache<EncodingId, EncodingInfo> encodingCache;
    
    private EncodingCache(String groupId, SchemaRegistryClient schemaRegistryClient) {
        encodingCache = CacheBuilder.newBuilder().build(new CacheLoader<EncodingId, EncodingInfo>() {
            @Override
            public EncodingInfo load(EncodingId key) {
                return schemaRegistryClient.getEncodingInfo(groupId, key);
            }
        });
    }
    
    @SneakyThrows(ExecutionException.class)
    public EncodingInfo getGroupEncodingInfo(EncodingId encodingId) {
        return encodingCache.get(encodingId);
    }

    @Synchronized
    public static EncodingCache getEncodingCacheForGroup(String groupId, SchemaRegistryClient schemaRegistryClient) {
        Key key = new Key(schemaRegistryClient, groupId);
        if (GROUP_CACHE_MAP.containsKey(key)) {
            return GROUP_CACHE_MAP.get(key);
        } else {
            EncodingCache value = new EncodingCache(groupId, schemaRegistryClient);
            GROUP_CACHE_MAP.put(key, value);
            return value;
        }
    }
    
    @Data
    private static class Key {
        private final SchemaRegistryClient client;
        private final String groupId;
    }
}
