/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostControllerStore;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Map;
import java.util.Set;

/**
 * This is a temporary implementation to directly query controller for table segment's location. 
 * Whenever table client is released, replace this class with table client.  
 */
public class HostStoreImpl implements HostControllerStore {
    private final ControllerImpl controller;

    public HostStoreImpl(ControllerImpl controller) {
        this.controller = controller;
    }

    @Override
    public Map<Host, Set<Integer>> getHostContainersMap() {
        throw new NotImplementedException("Host store");
    }

    @Override
    public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {
        throw new NotImplementedException("Host store");
    }

    @Override
    public int getContainerCount() {
        throw new NotImplementedException("Host store");
    }

    @Override
    public Host getHostForSegment(String scope, String stream, long segmentId) {
        throw new NotImplementedException("Host store");
    }

    @Override
    public Host getHostForTableSegment(String tableName) {
        return controller.getEndpointForSegment(tableName)
                         .thenApply(nodeUri -> new Host(nodeUri.getEndpoint(), nodeUri.getPort(), "")).join();
    }
}
