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

import com.google.common.collect.ImmutableMap;
import lombok.Data;

import java.util.Arrays;
import java.util.Objects;

@Data
public class SchemaInfo {
    private final String name;
    private final SchemaType schemaType;
    private final byte[] schemaData;
    private final ImmutableMap<String, String> properties;

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

        int result = Objects.hash(name, schemaType, properties);
        result = 31 * result + Arrays.hashCode(schemaData);
        return result;
    }
}
