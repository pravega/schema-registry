/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreException;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is a temporary implementation to directly query controller for table segment's location. 
 * Whenever table client is released, replace this class with table client.  
 */
class HostStoreImpl implements HostControllerStore {
    @Getter(AccessLevel.PACKAGE)
    private final ControllerImpl controller;
    private final LoadingCache<String, Host> cache;

    HostStoreImpl(ClientConfig clientConfig, ScheduledExecutorService executor) {
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), executor);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, Host>() {
            @Override
            public Host load(String tableName) throws Exception {
                return controller.getEndpointForSegment(tableName)
                          .thenApply(nodeUri -> new Host(nodeUri.getEndpoint(), nodeUri.getPort(), "")).join();
            }
        });
    }

    @Override
    public Map<Host, Set<Integer>> getHostContainersMap() {
        throw new UnsupportedOperationException("Host store");
    }

    @Override
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        throw new UnsupportedOperationException("Host store");
    }

    @Override
    public int getContainerCount() {
        throw new UnsupportedOperationException("Host store");
    }

    @Override
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        throw new UnsupportedOperationException("Host store");
    }

    @Override
    public Host getHostForTableSegment(String tableName) {
        try {
            return cache.get(tableName);
        } catch (Exception e) {
            throw new HostStoreException("Failed to contact controller");
        }
    }
    
    void invalidateCache(String tableName) {
        cache.invalidate(tableName);
    }
}
