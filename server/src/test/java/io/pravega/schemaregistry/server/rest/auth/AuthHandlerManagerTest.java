/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.auth;

import io.pravega.auth.AuthConstants;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AuthHandlerManagerTest {
    @Test
    public void testDisableBasicAuth() {
        ServiceConfig serviceConfig = ServiceConfig.builder()
                                                   .authEnabled(true)
                                                   .userPasswordFilePath(createAuthFile().getAbsolutePath())
                                                   .build();
        AuthHandlerManager manager = new AuthHandlerManager(serviceConfig);

        assertTrue(manager.getHandlerMap().containsKey(AuthConstants.BASIC));
        serviceConfig = ServiceConfig.builder().authEnabled(true).disablePasswordAuth(true)
                                                   .build();
        manager = new AuthHandlerManager(serviceConfig);
        assertFalse(manager.getHandlerMap().containsKey(AuthConstants.BASIC));
    }

    private File createAuthFile() {
        try {
            File authFile = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(authFile.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword("password");
                writer.write(String.format("%s:%s:%s%n", "admin", defaultPassword, "prn::*,READ_UPDATE;"));
            }
            return authFile;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }
}
