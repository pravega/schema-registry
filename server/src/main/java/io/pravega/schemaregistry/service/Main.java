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
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.StoreType;
import io.pravega.schemaregistry.storage.client.HostStoreImpl;
import io.pravega.schemaregistry.storage.client.TableStore;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class Main {
    public static void main(String[] args) {
        ServiceConfig config = ServiceConfig.builder().host(Config.SERVICE_HOST).port(Config.SERVICE_PORT).build();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(Config.PRAVEGA_CONTROLLER_URI)).build();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(Config.THREAD_POOL_SIZE);

        SchemaStore schemaStore;
        if (Config.STORE_TYPE.equals(StoreType.Pravega.name())) {
            ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
            HostControllerStore hostStore = new HostStoreImpl(clientConfig, executor);
            SegmentHelper segmentHelper = new SegmentHelper(connectionFactory, hostStore);
            TableStore tableStore = new TableStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), executor);
            schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, tableStore, executor);
        } else if (Config.STORE_TYPE.equals(StoreType.Pravega.name())) {
            schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        } else {
            throw new IllegalArgumentException(String.format("Store Type %s not supported", Config.STORE_TYPE));
        }
        
        SchemaRegistryService service = new SchemaRegistryService(schemaStore);

        RestServer restServer = new RestServer(service, config);
        restServer.startAsync();
        log.info("Awaiting start of REST server");
        restServer.awaitRunning();
        restServer.awaitTerminated();
    }
}
