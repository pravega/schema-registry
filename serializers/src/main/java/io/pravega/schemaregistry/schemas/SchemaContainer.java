/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.schemas;

import io.pravega.schemaregistry.contract.data.SchemaInfo;

/**
 * Interface for container class for schemas for different serialization formats. 
 * 
 * @param <T> Type of object. 
 */
public interface SchemaContainer<T> {
    SchemaInfo getSchemaInfo();
}
