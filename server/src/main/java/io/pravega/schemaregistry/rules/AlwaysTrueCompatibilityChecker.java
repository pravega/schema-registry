/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.rules;

import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.util.List;

public class AlwaysTrueCompatibilityChecker implements CompatibilityChecker {
    /**
     * Checks if readerSchema can be used to read data written using schema 1.
     * 
     * @param writerSchemas Writer schema. 
     * @param readerSchema Reader schema.
     * @return True if readerSchema can be used to read data written using schema 1, false otherwise. 
     */
    public boolean canRead(List<SchemaInfo> writerSchemas, SchemaInfo readerSchema) {
        return true;
    }

    /**
     * Checks if schema1 can be used to read data written using schema 2.
     *
     * @param writerSchema reader schema. 
     * @param readerSchemas writer schema.
     * @return True if schema1 can be used to read data written using schema 2, false otherwise. 
     */
    public boolean canBeRead(SchemaInfo writerSchema, List<SchemaInfo> readerSchemas) {
        return true;
        
    }

    /**
     * Checks if both schema1 and schema2 can be used to read data written with either. 
     *
     * @param schema1 Schema 1. 
     * @param schema2 Schema 2.
     * @return True if schema2 can read data written using schema 1 and schema 1 can read data written using schema 2. 
     * False otherwise.  
     */
    public boolean canMutuallyRead(SchemaInfo schema1, SchemaInfo schema2) {
        return true;
        
    }
}
