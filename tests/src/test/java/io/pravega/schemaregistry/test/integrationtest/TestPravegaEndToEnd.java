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
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import org.junit.Before;

import java.net.URI;

public class TestPravegaEndToEnd extends TestEndToEnd {
    ClientConfig clientConfig;
    
    @Before
    public void startPravega() {
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaStandaloneUtils.getControllerURI())).build();
    }
    
    SchemaStore getStore() {
        return SchemaStoreFactory.createPravegaStore(clientConfig, executor);
    }
}
