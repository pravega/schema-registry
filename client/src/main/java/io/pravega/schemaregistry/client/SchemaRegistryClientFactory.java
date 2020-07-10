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

/**
 * Factory class for creating Schema Registry client. 
 */
public class SchemaRegistryClientFactory {
    /**
     * Factory method to create Schema Registry Client with default namespace.
     * This sets the namespace context to use the default namespace (no namespace). 
     * 
     * @param config Configuration for creating registry client. 
     * @return SchemaRegistry client implementation
     */
    public static SchemaRegistryClient withDefaultNamespace(SchemaRegistryClientConfig config) {
        return new SchemaRegistryClientImpl(config, null);
    }
    
    /**
     * Factory method to create Schema Registry Client with namespace. 
     * This sets the namespace context for all calls to registry service. 
     * 
     * @param config Configuration for creating registry client. 
     * @param namespace Namespace 
     * @return SchemaRegistry client implementation
     */
    public static SchemaRegistryClient withNamespace(String namespace, SchemaRegistryClientConfig config) {
        return new SchemaRegistryClientImpl(config, namespace);
    }
}
