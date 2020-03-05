/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.storage.SchemaStore;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryServiceTest {
    SchemaRegistryService service;

    @Before
    public void setup() {
    }

    @Test
    public void testGroups() {
        SchemaStore store = mock(SchemaStore.class);
        SchemaRegistryService service = new SchemaRegistryService(store);

        ArrayList<String> groups = Lists.newArrayList("grp1", "grp2");
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new ListWithToken<>(groups, null));
        }).when(store).listGroups(any());
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new GroupProperties(SchemaType.Avro, 
                    new SchemaValidationRules(ImmutableList.of(), Compatibility.of(Compatibility.Type.Backward)), false, false));
        }).when(store).getGroupProperties(anyString());

        MapWithToken<String, GroupProperties> result = service.listGroups(null).join();
        assertEquals(result.getMap().size(), 2);
    }
}