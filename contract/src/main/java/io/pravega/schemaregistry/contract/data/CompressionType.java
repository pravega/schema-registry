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
 * Different types of compressions used for compressing data while writing it to the stream. 
 * A compression type and schema version combination uniquely identifies encoding format for the serialized data.
 * If a custom compression type which is not identified by the enum is desired by the application, it can be specified using
 * {@link CompressionType#custom} with {@link CompressionType#customTypeName}.  
 */
public enum CompressionType {
        None,
        Snappy,
        GZip,
        Custom;

        @Getter
        @Setter(AccessLevel.PRIVATE)
    private String customTypeName;

    public static CompressionType custom(String customTypeName) {
        CompressionType custom = CompressionType.Custom;
        custom.setCustomTypeName(customTypeName);
        return custom;
    }
}
