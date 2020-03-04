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

import com.google.common.collect.ImmutableList;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    public static final String GROUPS = "v1/groups";
    SchemaRegistryService service;

    @Override
    protected Application configure() {
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new SchemaRegistryResourceImpl(service));

        final RegistryApplication application = new RegistryApplication(resourceObjs);
        return application;
    }
    
    @Test
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SchemaType.of(SchemaType.Type.Avro), 
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                false, false);
        GroupProperties group2 = new GroupProperties(SchemaType.of(SchemaType.Type.Protobuf), 
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                false, false);

        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", group2);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, null));
        }).when(service).listGroups(eq(null));

        Future<Response> future = target(GROUPS).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        GroupsList list = response.readEntity(GroupsList.class);
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
