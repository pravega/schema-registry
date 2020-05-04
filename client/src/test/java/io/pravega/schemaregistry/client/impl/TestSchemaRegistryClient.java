/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestSchemaRegistryClient {
    @Test
    public void testGroup() {
        Client jerseyClient = mock(Client.class);
        WebTarget webTarget = mock(WebTarget.class);
        Invocation.Builder invocation = mock(Invocation.Builder.class);

        doReturn(webTarget).when(jerseyClient).target(any(URI.class));
        doReturn(webTarget).when(webTarget).path(anyString());
        doReturn(invocation).when(webTarget).request(MediaType.APPLICATION_JSON);

        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(URI.create("http://localhost:9092"), jerseyClient);
        Response response = mock(Response.class);
        
        // add group
        // 1. success response code
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        boolean addGroup = client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        assertTrue(addGroup);
        
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        addGroup = client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        assertFalse(addGroup);

        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", 
                () -> client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap()),
                e -> e instanceof RuntimeException);
        reset(response);
        reset(invocation);
        
        // list groups
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().groupName("mygroup").properties(Collections.emptyMap())
                                                       .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                                                               .schemaType(io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                                                       .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                                                       .validateByObjectType(false);
        GroupsList groupList = new GroupsList().groups(Collections.singletonList(mygroup));
        doReturn(groupList).when(response).readEntity(eq(GroupsList.class));

        Map<String, io.pravega.schemaregistry.contract.data.GroupProperties> groups = client.listGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.containsKey("mygroup"));
        assertEquals(groups.get("mygroup").getSchemaType(), SchemaType.Any);
        assertEquals(groups.get("mygroup").getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()), Compatibility.backward());

        reset(response);
        reset(invocation);
    }
}
