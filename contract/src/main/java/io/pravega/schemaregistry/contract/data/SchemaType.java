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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Different types of serialization formats used for serializing data. 
 * Registry supports Avro, Protobuf and Json schema types but any custom type could be used with the registry using custom type. 
 *
 * If a schema type is not present in the enum it can be specified using {@link SchemaType#custom} with {@link SchemaType#customTypeName}. 
 * Allowed values of {@link Compatibility} mode with custom type are AllowAny or DenyAll.
 */

public enum SchemaType {
        None,
        Avro,
        Protobuf,
        Json,
        Custom;

        @Getter
        @Setter(AccessLevel.PRIVATE)
    private String customTypeName;

    public static SchemaType custom(String customTypeName) {
        SchemaType type = SchemaType.Custom;
        type.setCustomTypeName(customTypeName);
        return type;
    }
}
