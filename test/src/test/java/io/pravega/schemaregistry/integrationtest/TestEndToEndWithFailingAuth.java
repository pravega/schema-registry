/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.integrationtest;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.controller.server.security.auth.StrongPasswordProcessor;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.common.CredentialProvider;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TestEndToEndWithFailingAuth extends TestEndToEndWithAuth {
    @Rule
    public Timeout globalTimeout = new Timeout(3, TimeUnit.MINUTES);
    
    @Override
    public SchemaRegistryClient getClient() {
        return SchemaRegistryClientFactory.withDefaultNamespace(
                SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:" + port))
                                          .authentication(new CredentialProvider.DefaultCredentialProvider("Basic", 
                                                  Base64.getEncoder().encodeToString(("a" + ":" + "b").getBytes(Charsets.UTF_8))))
                                          .build());
    }

    @Override
    @Test
    public void testEndToEnd() {
        AssertExtensions.assertThrows("", super::testEndToEnd,
            e -> e instanceof RegistryExceptions.UnauthorizedException);
    }

    @Override
    @Test
    public void testLargeSchemas() {
        AssertExtensions.assertThrows("", super::testLargeSchemas,
            e -> e instanceof RegistryExceptions.UnauthorizedException);
    }
}

