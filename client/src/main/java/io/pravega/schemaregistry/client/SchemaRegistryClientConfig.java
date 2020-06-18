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

import lombok.Builder;
import lombok.Data;

import java.net.URI;

/**
 * Registry client configuration used to create registry client. 
 */
@Data
@Builder
public class SchemaRegistryClientConfig {
    /**
     * URI for connecting with registry client.
     */
    private final URI schemaRegistryUri;
    private final String namespace;
    private final boolean authEnabled;
    private final String authMethod;
    private final String authToken;

    private SchemaRegistryClientConfig(URI schemaRegistryUri, String namespace, boolean authEnabled, String authMethod, String authToken) {
        this.schemaRegistryUri = schemaRegistryUri;
        this.namespace = namespace;
        this.authEnabled = authEnabled;
        this.authMethod = authMethod;
        this.authToken = authToken;
    }

    public static final class SchemaRegistryClientConfigBuilder {
        private boolean authEnabled = false;
        private String namespace = null;
    }
}
