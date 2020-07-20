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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.nio.ByteBuffer;

/**
 * Encapsulates properties of a schema. 
 * {@link SchemaInfo#type} object type represented by the schema. This is used to identify the exact object type. 
 * If (ref: {@link GroupProperties#allowMultipleTypes}) is set to true, the group will allow multiple schemas to coexist. 
 * {@link SchemaInfo#serializationFormat} Serialization format.
 * {@link SchemaInfo#schemaData} Schema as an array of 8-bit unsigned bytes. This is schema-type specific and to be consumed
 * by schema-type specific parsers. 
 * {@link SchemaInfo#properties} A key value map of strings where user defined metadata can be recorded with schemas.
 * This is not interpreted by the registry service or client and can be used by applications for sharing any additional 
 * application specific information with the schema.
 */
@Data
@Builder
public class SchemaInfo {
    public static final int ONE_MB = 1024 * 1024;
    /**
     * Identifies the object type that is represented by the schema. 
     */
    private @NonNull final String type;
    /**
     * Serialization format that this schema is intended to be used for. 
     */
    private @NonNull final SerializationFormat serializationFormat;
    /**
     * Schema as an array of 8-bit unsigned bytes. 
     */
    private @NonNull final ByteBuffer schemaData;
    /**
     * User defined key value strings that users can use to add any additional metadata to the schema. 
     */
    private @NonNull final ImmutableMap<String, String> properties;

    public SchemaInfo(@NonNull String type, @NonNull SerializationFormat serializationFormat, @NonNull ByteBuffer schemaData, @NonNull ImmutableMap<String, String> properties) {
        Preconditions.checkArgument(serializationFormat != SerializationFormat.Any, "Invalid Serialization Format.");
        Preconditions.checkArgument(schemaData.remaining() <= 8 * ONE_MB, "Invalid schema binary.");
        Preconditions.checkArgument(properties.size() <= 100 && 
                        properties.entrySet().stream().allMatch(x -> x.getKey().length() <= 200 && x.getValue().length() <= 200),
                "Invalid properties, make sure each key and value are less than or equal to 200 bytes and there are no more than 100 entries.");
        this.type = type;
        this.serializationFormat = serializationFormat;
        this.schemaData = schemaData;
        this.properties = properties;
    }
    
    public static class SchemaInfoBuilder implements ObjectBuilder<SchemaInfo> {
    }
}
