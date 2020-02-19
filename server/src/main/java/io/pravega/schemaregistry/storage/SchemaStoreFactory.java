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

import io.pravega.schemaregistry.storage.impl.InMemoryScopes;
import io.pravega.schemaregistry.storage.impl.SchemaStoreImpl;
import org.apache.commons.lang3.NotImplementedException;

public class SchemaStoreFactory {
    public static SchemaStore createStore(StoreType type) {
        switch (type) {
            case InMemory:
                return new SchemaStoreImpl(new InMemoryScopes());
            case Pravega:
            default:
                throw new NotImplementedException("pravega store not implemented yet");
        }
    }
}
