/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.schemaregistry.storage.impl.InMemoryNamespaces;
import io.pravega.schemaregistry.storage.impl.SchemaStoreImpl;
import org.apache.commons.lang3.NotImplementedException;

import java.util.concurrent.ScheduledExecutorService;

public class SchemaStoreFactory {
    public static SchemaStore createStore(StoreType type, ScheduledExecutorService executor) {
        switch (type) {
            case InMemory:
                return new SchemaStoreImpl(new InMemoryNamespaces(executor));
            case Pravega:
            default:
                throw new NotImplementedException("Pravega tables based store coming soon");
        }
    }
}
