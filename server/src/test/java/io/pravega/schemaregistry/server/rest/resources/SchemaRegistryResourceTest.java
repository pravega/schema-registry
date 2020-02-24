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
import com.google.common.collect.Lists;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateNamespaceRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespacesList;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    public static final String NAMESPACES = "v1/namespaces";
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
    public void namespaces() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(new ListWithToken<>(Lists.newArrayList("namespace1", "namespace2"), null)))
                .when(service).listNamespaces(eq(null));

        Future<Response> future = target(NAMESPACES).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        NamespacesList entity = response.readEntity(NamespacesList.class);
        assertEquals(entity.getNamespaces().size(), 2);
        assertTrue(entity.getNamespaces().stream().anyMatch(x -> x.getNamespaceName().equals("namespace1")));
        assertTrue(entity.getNamespaces().stream().anyMatch(x -> x.getNamespaceName().equals("namespace2")));

        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(service).createNamespace(anyString());

        CreateNamespaceRequest createNamespaceRequest = new CreateNamespaceRequest().namespaceName("namespace");
        future = target(NAMESPACES).request().async().post(Entity.entity(createNamespaceRequest, MediaType.APPLICATION_JSON));
        response = future.get();
        assertEquals(response.getStatus(), 201);

        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(service).deleteNamespace(eq("namespace"));

        future = target(NAMESPACES + "/" + "namespace").request().async().delete();
        response = future.get();
        assertEquals(response.getStatus(), 200);
    }
    
    @Test
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SchemaType.of(SchemaType.Type.Avro), 
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                false, false);
        GroupProperties group2 = new GroupProperties(SchemaType.of(SchemaType.Type.Protobuf), 
                new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), 
                false, false);

        String namespace = "namespace";
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", group2);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, null));
        }).when(service).listGroupsInNamespace(eq(namespace), eq(null));

        Future<Response> future = target(NAMESPACES + "/" + namespace + "/groups").request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        GroupsList list = response.readEntity(GroupsList.class);
        assertEquals(list.getGroups().size(), 2);
        assertEquals(list.getGroups().size(), 2);
    } 
}
