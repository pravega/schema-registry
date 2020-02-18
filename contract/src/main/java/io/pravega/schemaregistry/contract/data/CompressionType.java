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

/**
 * Different types of compressions used for compressing data while writing it to the stream. 
 * A compression type and schema version combination uniquely identifies encoding format for the serialized data.
 * If a custom compression type which is not identified by the enum is desired by the application, it can be specified using
 * {@link Type#custom} with {@link CompressionType#customTypeName}.  
 */
@Data
public class CompressionType {
    public enum Type {
        None,
        Snappy,
        GZip,
        Custom;
    }

    private final Type compressionType;
    private final String customTypeName;

    private CompressionType(Type compressionType, String customTypeName) {
        this.compressionType = compressionType;
        this.customTypeName = customTypeName;
    }

    public static CompressionType of(Type type) {
        return new CompressionType(type, null);
    }

    public static CompressionType custom(String customTypeName) {
        return new CompressionType(Type.Custom, customTypeName);
    }
}
