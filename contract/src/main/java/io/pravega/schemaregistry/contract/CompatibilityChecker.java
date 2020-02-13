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
    /**
     * Checks if schema2 can be used to read data written using schema 1.
     * 
     * @param schema1 Writer schema. 
     * @param schema2 Reader schema.
     * @return True if schema2 can be used to read data written using schema 1, false otherwise. 
     */
    boolean canRead(List<SchemaRegistryContract.SchemaInfo> schema1, SchemaRegistryContract.SchemaInfo schema2);

    /**
     * Checks if schema1 can be used to read data written using schema 2.
     *
     * @param schema1 reader schema. 
     * @param schema2 writer schema.
     * @return True if schema1 can be used to read data written using schema 2, false otherwise. 
     */
    boolean canBeRead(SchemaRegistryContract.SchemaInfo schema1, List<SchemaRegistryContract.SchemaInfo> schema2);

    /**
     * Checks if both schema1 and schema2 can be used to read data written with either. 
     *
     * @param schema1 Schema 1. 
     * @param schema2 Schema 2.
     * @return True if schema2 can read data written using schema 1 and schema 1 can read data written using schema 2. 
     * False otherwise.  
     */
    boolean canMutuallyRead(SchemaRegistryContract.SchemaInfo schema1, SchemaRegistryContract.SchemaInfo schema2);
}
