/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import lombok.Data;

@Data
public class SchemaType {
    public enum Type {
        None,
        Avro,
        Protobuf,
        Json,
        Custom;
    }

    private final Type schemaType;
    private final String customTypeName;

    private SchemaType(Type schemaType, String customTypeName) {
        this.schemaType = schemaType;
        this.customTypeName = customTypeName;
    }

    public static SchemaType of(Type type) {
        return new SchemaType(type, null);
    }

    public static SchemaType custom(String customTypeName) {
        return new SchemaType(Type.Custom, customTypeName);
    }
}
