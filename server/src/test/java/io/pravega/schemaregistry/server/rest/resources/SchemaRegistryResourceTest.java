/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
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
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    private static final String GROUPS = "v1/groups";
    private SchemaRegistryService service;

    @Override
    protected Application configure() {
        forceSet(TestProperties.CONTAINER_PORT, "0");
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new SchemaRegistryResourceImpl(service, ServiceConfig.builder().build()));

        return new RegistryApplication(resourceObjs);
    }

    @Test
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", null);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, null));
        }).when(service).listGroups(any(), anyInt());

        Future<Response> future = target(GROUPS).queryParam("limit", 100).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 2);

        // region create group
        // endregion

        // region delete group
        // endregion
    }

    @Test
    public void groupProperties() throws ExecutionException, InterruptedException {
        // region group properties
        // endregion 

        // region update validation rules
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
        doAnswer(x -> CompletableFuture.completedFuture(true)).when(service).canRead(any(), any());
        CanReadRequest canReadRequest = new CanReadRequest().schemaInfo(new SchemaInfo()
                .schemaName("name")
                .schemaType(ModelHelper.encode(SchemaType.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap())
        );
        Future<Response> future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                                                       .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());

        canReadRequest = new CanReadRequest().schemaInfo(new SchemaInfo()
                .schemaName("name")
                .schemaData(new byte[0])
                .properties(Collections.emptyMap())
        );
        future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON));
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
