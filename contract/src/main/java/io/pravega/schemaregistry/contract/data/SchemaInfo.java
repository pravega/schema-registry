/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;

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
    /**
     * Identifies the object type that is represented by the schema. 
     */
    private final String type;
    /**
     * Serialization format that this schema is intended to be used for. 
     */
    private final SerializationFormat serializationFormat;
    /**
     * Schema as an array of 8-bit unsigned bytes. 
     */
    private final ByteBuffer schemaData;
    /**
     * User defined key value strings that users can use to add any additional metadata to the schema. 
     */
    private final ImmutableMap<String, String> properties;

    public SchemaInfo(String type, SerializationFormat serializationFormat, ByteBuffer schemaData, ImmutableMap<String, String> properties) {
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(serializationFormat != SerializationFormat.Any);
        this.type = type;
        this.serializationFormat = serializationFormat;
        this.schemaData = schemaData;
        this.properties = properties;
    }
    
    public static class SchemaInfoBuilder implements ObjectBuilder<SchemaInfo> {
    }
}
