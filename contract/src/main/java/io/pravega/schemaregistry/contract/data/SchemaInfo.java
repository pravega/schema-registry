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

import io.pravega.common.ObjectBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates properties of a schema. 
 * {@link SchemaInfo#name} object type represented by the schema. This is used to identify the exact object type 
 * and used if a group needs to be divided in different evolution subsets by object types. (ref: {@link GroupProperties#versionBySchemaName}). 
 * {@link SchemaInfo#schemaType} Serialization format.
 * {@link SchemaInfo#schemaData} Schema as an array of 8-bit unsigned bytes. This is schema-type specific and to be consumed
 * by schema-type specific parsers. 
 * {@link SchemaInfo#properties} A key value map of strings where user defined metadata can be recorded with schemas.
 * This is not interpreted by the registry service or client and can be used by applications for sharing any additional 
 * application specific information with the schema.
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaInfo {
    private final String name;
    private final SchemaType schemaType;
    private final byte[] schemaData;
    private final Map<String, String> properties;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaInfo that = (SchemaInfo) o;
        return Objects.equals(name, that.name) &&
                schemaType == that.schemaType &&
                Arrays.equals(schemaData, that.schemaData) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {

        int result = Objects.hash(name, name, schemaType, properties);
        result = 31 * result + Arrays.hashCode(schemaData);
        return result;
    }

    public static class SchemaInfoBuilder implements ObjectBuilder<SchemaInfo> {
    }
}
