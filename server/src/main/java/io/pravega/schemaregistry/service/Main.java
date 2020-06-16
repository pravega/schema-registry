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

import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.StoreType;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class Main {
    public static void main(String[] args) {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(Config.PRAVEGA_CONTROLLER_URI)).build();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(Config.THREAD_POOL_SIZE);

        SchemaStore schemaStore;
        ServiceConfig serviceConfig = Config.SERVICE_CONFIG;
        if (Config.STORE_TYPE.equals(StoreType.Pravega.name())) {
            schemaStore = SchemaStoreFactory.createPravegaStore(serviceConfig, clientConfig, executor);
        } else if (Config.STORE_TYPE.equals(StoreType.InMemory.name())) {
            schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        } else {
            throw new IllegalArgumentException(String.format("Store Type %s not supported", Config.STORE_TYPE));
        }
        
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);

        RestServer restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        log.info("Awaiting start of REST server");
        restServer.awaitRunning();
        restServer.awaitTerminated();
    }
}
