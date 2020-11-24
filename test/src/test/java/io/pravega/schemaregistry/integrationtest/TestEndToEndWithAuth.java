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

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.CredentialProvider;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.concurrent.Executors;

@Slf4j
public class TestEndToEndWithAuth extends TestInMemoryEndToEnd {
    private final static String SYSTEM_ADMIN = "admin";
    private final static String PASSWORD = "1111_aaaa";

    protected int port;
    private RestServer restServer;
    private File authFile;

    @Before
    @Override
    public void setUp() {
        executor = Executors.newScheduledThreadPool(Config.THREAD_POOL_SIZE);

        port = TestUtils.getAvailableListenPort();
        authFile = createAuthFile();
        ServiceConfig serviceConfig = ServiceConfig.builder().port(port)
                                                   .authEnabled(true)
                                                   .userPasswordFilePath(authFile.getAbsolutePath())
                                                   .build();
        SchemaStore store = getStore();

        SchemaRegistryService service = new SchemaRegistryService(store, executor);

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();
    }

    @After
    public void tearDown() {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
        authFile.delete();
    }

    @Override
    public SchemaRegistryClient newClient() {
        return SchemaRegistryClientFactory.withDefaultNamespace(
                SchemaRegistryClientConfig.builder().schemaRegistryUri(URI.create("http://localhost:" + port))
                                          .authentication(new CredentialProvider.DefaultCredentialProvider("Basic", 
                                                  Base64.getEncoder().encodeToString((SYSTEM_ADMIN + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                          .build());
    }
    
    private File createAuthFile() {
        try {
            File authFile = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(authFile.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword(PASSWORD);
                writer.write(credentialsAndAclAsString(SYSTEM_ADMIN, defaultPassword, "prn::*,READ_UPDATE;"));
            }
            return authFile;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private String credentialsAndAclAsString(String username, String password, String acl) {
        // This will return a string that looks like this:"<username>:<pasword>:acl\n"
        return String.format("%s:%s:%s%n", username, password, acl);
    }

}

