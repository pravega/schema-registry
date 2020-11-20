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

import io.pravega.schemaregistry.common.CredentialProvider;
import org.junit.Test;

import static org.junit.Assert.*;

public class SchemaRegistryClientConfigTest {
    @Test
    public void testSSLConfig() {
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().build();
        assertNull(config.getCertificate());
        assertNull(config.getTrustStore());
        assertNull(config.getTrustStoreType());
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().certificate("certPath").build();
        assertEquals(config.getCertificate(), "certPath");
        assertNull(config.getTrustStorePassword());
        assertNull(config.getTrustStoreType());
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().trustStore("trustStorePath", "JKS", null).build();
        assertNull(config.getCertificate());
        assertEquals(config.getTrustStore(), "trustStorePath");
        assertEquals(config.getTrustStoreType(), "JKS");
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().trustStore("trustStorePath", "JKS", "password").build();
        assertNull(config.getCertificate());
        assertEquals(config.getTrustStore(), "trustStorePath");
        assertEquals(config.getTrustStoreType(), "JKS");
        assertEquals(config.getTrustStorePassword(), "password");
    }

    @Test
    public void testAuthConfig() {
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().build();
        assertFalse(config.isAuthEnabled());

        config = SchemaRegistryClientConfig.builder().authentication(new CredentialProvider() {
            @Override
            public String getMethod() {
                return "method";
            }

            @Override
            public String getToken() {
                return "token";
            }
        }).build();
        assertTrue(config.isAuthEnabled());
        assertEquals("method", config.getCredentialProvider().getMethod());
        assertEquals("token", config.getCredentialProvider().getToken());
    }
}
