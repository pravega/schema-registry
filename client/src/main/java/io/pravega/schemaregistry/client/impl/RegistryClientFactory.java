/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import org.apache.commons.lang3.NotImplementedException;

public class RegistryClientFactory {
    public SchemaRegistryClient createRegistryClient(RegistryClientConfig config) {
        // TODO: create rest client for registry. 
        throw new NotImplementedException("create registry client");
    }
}
