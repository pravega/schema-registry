/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigTest {
    @Test
    public void testConfig() {
        ServiceConfig config = ServiceConfig.builder().build();
        assertEquals(config.getHost(), "0.0.0.0");
        assertEquals(config.getPort(), 9092);
        assertFalse(config.isAuthEnabled());
        assertFalse(config.isTlsEnabled());

        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ServiceConfig.builder().tlsEnabled(true).build());
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> ServiceConfig.builder().tlsEnabled(true)
                                                                                         .tlsCertFilePath("a").build());
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> 
                ServiceConfig.builder().tlsEnabled(true).tlsCertFilePath("a").serverKeyStoreFilePath("a").build());
        config = ServiceConfig.builder().tlsEnabled(true).tlsCertFilePath("a").serverKeyStoreFilePath("a").tlsKeyStorePasswordFilePath("a").build();
        assertTrue(config.isTlsEnabled());
    }
}
