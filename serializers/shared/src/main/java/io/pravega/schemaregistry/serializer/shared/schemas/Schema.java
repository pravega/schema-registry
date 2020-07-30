/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.schemas;

import io.pravega.schemaregistry.contract.data.SchemaInfo;

/**
 * Interface for container class for schemas for different serialization formats. 
 * 
 * @param <T> Type of object. 
 */
public interface Schema<T> {
    /**
     * Returns the {@link SchemaInfo} object that is computed from the schema object. SchemaInfo is the object that encapsulates
     * all schema metadata to be shared with the schema registry service.
     *
     * @return Schema Info object derived from the schema object.
     */
    SchemaInfo getSchemaInfo();

    /**
     * Class for the Type of object.
     * 
     * @return Class of type T
     */
    Class<T> getTClass();
}
