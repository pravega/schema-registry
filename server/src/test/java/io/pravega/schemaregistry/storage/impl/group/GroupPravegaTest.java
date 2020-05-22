/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.client.ClientConfig;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GroupPravegaTest {
    private ScheduledExecutorService executor;
    String groupId;
    ClientConfig clientConfig;
    PravegaKVGroupTable pravegaKVGroupTable;
    TableStore tableStore;
    GrpcAuthHelper grpcAuthHelper;
    Group<Version> group;

    @Before
    public void setUp(){
        grpcAuthHelper = new GrpcAuthHelper(Boolean.FALSE, null, null);
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        clientConfig = ClientConfig.builder().controllerURI(URI.create(pravegaStandaloneUtils.getControllerURI())).build();
        groupId = "mygroup";
        executor = Executors.newScheduledThreadPool(5);
        tableStore = new TableStore(clientConfig, () -> "", executor);
        pravegaKVGroupTable = new PravegaKVGroupTable(groupId, "tableStore", tableStore);
        group = new Group<>(groupId, pravegaKVGroupTable,executor);
    }

    @After
    public void tearDown(){
        executor.shutdownNow();
    }


}
