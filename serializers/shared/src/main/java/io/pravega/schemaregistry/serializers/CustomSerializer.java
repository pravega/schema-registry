/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.OutputStream;

public interface CustomSerializer<T> {
    void serialize(T var, SchemaInfo schema, OutputStream outputStream);
}
