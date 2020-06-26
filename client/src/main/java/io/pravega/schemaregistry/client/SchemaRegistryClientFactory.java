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
     * Factory method to create Schema Registry Client.
     * 
     * @param config Configuration for creating registry client. 
     * @return SchemaRegistry client implementation
     */
    public static SchemaRegistryClient createRegistryClient(SchemaRegistryClientConfig config) {
        return new SchemaRegistryClientImpl(config);
    }
}
