/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import com.google.common.collect.Lists;
import io.pravega.schemaregistry.common.CredentialProvider;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import javax.ws.rs.ProcessingException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class SchemaRegistryClientAuthTest {
    
    @Test
    @SuppressWarnings("unchecked")
    public void testAuthFilter() {
        URI schemaRegistryUri = URI.create("http://localhost:9092");

        AtomicLong methodCounter = new AtomicLong(0);
        AtomicLong tokenCounter = new AtomicLong(0);
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
                                                                      .authentication(new CredentialProvider() {
                                                                          @Override
                                                                          public String getMethod() {
                                                                              methodCounter.getAndIncrement();
                                                                              return "MyMethod";
                                                                          }

                                                                          @Override
                                                                          public String getToken() {
                                                                              tokenCounter.getAndIncrement();
                                                                              return "myToken";
                                                                          }
                                                                      })
                                                                      .schemaRegistryUri(schemaRegistryUri)
                                                                      .build();

        SchemaRegistryClient client = SchemaRegistryClientFactory.withDefaultNamespace(config);

        // verify that the group request actually contained the auth header
        // Note: the call to server will fail as there is no server listening. We are merely verifying that our
        // request filter is invoked before sending the request to the server. 
        AssertExtensions.assertThrows("", () -> Lists.newArrayList(client.listGroups()), 
            e -> e instanceof ProcessingException);
        // verify that our credential provider is invoked
        assertEquals(1, methodCounter.get());
        assertEquals(1, tokenCounter.get());

        // verify that subsequent calls increment the counters. 
        AssertExtensions.assertThrows("", () -> Lists.newArrayList(client.listGroups()),
                e -> e instanceof ProcessingException);

        // verify that our credential provider is invoked
        assertEquals(2, methodCounter.get());
        assertEquals(2, tokenCounter.get());
    }
}
