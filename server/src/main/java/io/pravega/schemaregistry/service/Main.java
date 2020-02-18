/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.StoreType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        // TODO: read config host and port from service configuration
        ServiceConfig config = ServiceConfig.builder().host("localhost").port(1234).build();
        SchemaStore schemaStore = SchemaStoreFactory.createStore(StoreType.InMemory);
        SchemaRegistryService service = new SchemaRegistryService(schemaStore);
        RestServer restServer = new RestServer(service, config);
        restServer.startAsync();
        log.info("Awaiting start of REST server");
        restServer.awaitRunning();
        restServer.awaitTerminated();
    }
}
