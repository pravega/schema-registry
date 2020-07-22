/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import com.google.common.collect.Lists;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.server.rest.filter.NamespaceRedirectFilter;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    private static final String GROUPS = "v1/groups";
    private static final String NAMESPACE_FORMAT = "v1/namespace/%s/groups";
    private SchemaRegistryService service;
    private ScheduledExecutorService executor;

    @Override
    protected Application configure() {
        executor = Executors.newSingleThreadScheduledExecutor();
        forceSet(TestProperties.CONTAINER_PORT, "0");
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new NamespaceRedirectFilter());
        ServiceConfig config = ServiceConfig.builder().build();
        AuthHandlerManager authHandlerManager = new AuthHandlerManager(config);
        resourceObjs.add(new GroupResourceImpl(service, config, authHandlerManager, executor));
        resourceObjs.add(new SchemaResourceImpl(service, config, authHandlerManager, executor));

        return new RegistryApplication(resourceObjs);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();    
    }
    
    @Test
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(),
                false);
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            return CompletableFuture.completedFuture(new ResultPage<>(Lists.newArrayList(map.entrySet()), null));
        }).when(service).listGroups(any(), any(), anyInt());

        Future<Response> future = target(GROUPS).queryParam("limit", 100).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 1);
        
        future = target(String.format(NAMESPACE_FORMAT, "ns")).queryParam("limit", 100).request().async().get();
        response = future.get();
        assertEquals(response.getStatus(), 200);
        list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 1);

        // region create group
        // endregion

        // region delete group
        // endregion
    }

    @Test
    public void groupProperties() throws ExecutionException, InterruptedException {
        // region group properties
        // endregion 

        // region update compatibility
        // endregion
    }

    @Test
    public void groupSchemas() throws ExecutionException, InterruptedException {
        // region get all schemas
        // endregion

        // region get latest schema
        // endregion

        // region add new schema
        // endregion

        // can read
        doAnswer(x -> CompletableFuture.completedFuture(true)).when(service).canRead(any(), any(), any());
        SchemaInfo schemaInfo = new SchemaInfo()
                .type("name")
                .serializationFormat(ModelHelper.encode(SerializationFormat.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap());
        Future<Response> future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                                                       .post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());

        schemaInfo = new SchemaInfo()
                .type("name")
                .schemaData(new byte[0])
                .properties(Collections.emptyMap());
        future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                .post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON));
        response = future.get();
        assertEquals(400, response.getStatus());
    }

    @Test
    public void schemaVersion() throws ExecutionException, InterruptedException {

        // region get schema from version 
        // endregion

        // region validate schema
        // endregion

        // region get schema version
        // endregion
    }

    @Test
    public void encoding() throws ExecutionException, InterruptedException {
        // region get encoding id
        // endregion

        // region get encoding info
        // endregion
    }
}
