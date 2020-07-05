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

/**
 * BackwardAndForward checker interface to compare a schema against existing schemas for compatibility. 
 */
public interface CompatibilityChecker {
    /**
     * Checks if readUsing can be used to read data written using all schemas in writtenUsing.
     * 
     * @param readUsing Schema used while reading the data.     
     * @param writtenUsing Schema used for writing the data. 
     * @return True if readUsing can be used to read data written using writtenUsing, false otherwise. 
     */
    boolean canRead(SchemaInfo readUsing, List<SchemaInfo> writtenUsing);

    /**
     * Checks if all schemas in readUsing can be used to read data written using writtenUsing.
     *
     * @param writtenUsing Schema used for writing the data. 
     * @param readUsing Schema used while reading the data.
     * @return True if any of readUsing can be used to read data written using writtenUsing, false otherwise. 
     */
    boolean canBeRead(SchemaInfo writtenUsing, List<SchemaInfo> readUsing);

    /**
     * Checks if both schema and schemaList can be used to read data written with either. 
     *
     * @param schema Schema to check. 
     * @param schemaList All schemas to check against.
     * @return True if schema can read and be readby all schemas in the schemas, false otherwise.  
     */
    boolean canMutuallyRead(SchemaInfo schema, List<SchemaInfo> schemaList);
}
