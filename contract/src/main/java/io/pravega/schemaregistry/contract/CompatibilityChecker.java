/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract;

import java.util.List;

public interface CompatibilityChecker {
    boolean canRead(List<SchemaRegistryContract.SchemaInfo> writerSchemas, SchemaRegistryContract.SchemaInfo readerSchema);
    boolean canBeRead(SchemaRegistryContract.SchemaInfo writerSchema, List<SchemaRegistryContract.SchemaInfo> readerSchemas);
    boolean canMutuallyRead(SchemaRegistryContract.SchemaInfo schema1, SchemaRegistryContract.SchemaInfo schema2);
}
