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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Internal class that is responsible for querying controller for hosts for table segments. 
 * It maintains a cache of already found hosts. 
 */
class HostStore {
    @Getter(AccessLevel.PACKAGE)
    private final ControllerImpl controller;
    private final Cache<String, Controller.NodeUri> cache;

    HostStore(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), executor);
        this.cache = CacheBuilder.newBuilder()
                    .maximumSize(Config.TABLE_SEGMENT_CACHE_SIZE)
                    .build();
    }

    CompletableFuture<Controller.NodeUri> getHostForTableSegment(String tableName) {
        try {
            Controller.NodeUri uri = cache.getIfPresent(tableName);
            if (uri == null) {
                return controller.getEndpointForSegment(tableName)
                                 .thenApply(u -> {
                                     Controller.NodeUri nodeUri = Controller.NodeUri.newBuilder().setEndpoint(u.getEndpoint()).setPort(u.getPort()).build();
                                     cache.put(tableName, nodeUri);
                                     return nodeUri;
                                 })
                        .exceptionally(e -> {
                            throw StoreExceptions.create(StoreExceptions.Type.CONNECTION_ERROR, "Failed to contact controller");
                        });
            } else {
                return CompletableFuture.completedFuture(uri);
            }
        } catch (Exception e) {
            return Futures.failedFuture(e);
        }
    }
    
    void invalidateCache(String tableName) {
        cache.invalidate(tableName);
    }
}
