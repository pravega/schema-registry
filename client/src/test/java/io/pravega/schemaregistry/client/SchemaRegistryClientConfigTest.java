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

import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SchemaRegistryClientConfigTest {
    @Test
    public void testSSLConfig() {
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().build();
        assertTrue(config.isSystemPropTls());
        assertFalse(config.isCertificateTrustStore());
        assertNull(config.getTrustStore());
        assertNull(config.getTrustStoreType());
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().certificateTrustStore("certPath").build();
        assertFalse(config.isSystemPropTls());
        assertTrue(config.isCertificateTrustStore());
        assertEquals(config.getTrustStore(), "certPath");
        assertNull(config.getTrustStoreType());
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().trustStore("trustStorePath", "JKS", null).build();
        assertFalse(config.isSystemPropTls());
        assertFalse(config.isCertificateTrustStore());
        assertEquals(config.getTrustStore(), "trustStorePath");
        assertEquals(config.getTrustStoreType(), "JKS");
        assertNull(config.getTrustStorePassword());

        config = SchemaRegistryClientConfig.builder().trustStore("trustStorePath", "JKS", "password").build();
        assertFalse(config.isSystemPropTls());
        assertFalse(config.isCertificateTrustStore());
        assertEquals(config.getTrustStore(), "trustStorePath");
        assertEquals(config.getTrustStoreType(), "JKS");
        assertEquals(config.getTrustStorePassword(), "password");
    }

    @Test
    public void testAuthConfig() {
        SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder().build();
        assertFalse(config.isAuthEnabled());

        config = SchemaRegistryClientConfig.builder().authentication("method", "token").build();
        assertTrue(config.isAuthEnabled());
        assertEquals(config.getAuthMethod(), "method");
        assertEquals(config.getAuthToken(), "token");
    }
}
