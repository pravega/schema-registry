/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.client.HostStoreImpl;
import io.pravega.schemaregistry.storage.client.TableStore;
import org.junit.After;
import org.junit.Before;

import java.net.URI;

public class TestPravegaEndToEnd extends TestEndToEnd {
    LocalPravegaEmulator localPravega;
    ClientConfig clientConfig;
    
    @Before
    public void setup() throws Exception {
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator
                .builder()
                .controllerPort(9090)
                .segmentStorePort(1234)
                .zkPort(2180)
                .restServerPort(9091)
                .enableRestServer(false)
                .enableAuth(false)
                .enableTls(false);

        localPravega = emulatorBuilder.build();
        localPravega.getInProcPravegaCluster().start();

        clientConfig = ClientConfig.builder().controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI())).build();
    }
    
    @After
    public void teardown() throws Exception {
        localPravega.close();
    }
    
    SchemaStore getStore() {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(clientConfig);
        HostControllerStore hostStore = new HostStoreImpl(clientConfig, executor);
        SegmentHelper segmentHelper = new SegmentHelper(connectionFactory, hostStore);
        TableStore tableStore = new TableStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), executor);
        return SchemaStoreFactory.createPravegaStore(clientConfig, tableStore, executor);
    }
}
