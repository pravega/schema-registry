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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Different types of serialization formats used for serializing data. 
 * Registry supports Avro, Protobuf and Json schema types but any custom type could be used with the registry using custom type. 
 *
 * If a custom schema type not identified by the {@link Type} enum is desired by the application, it can be specified using
 * {@link Type#custom} with {@link SchemaType#customTypeName}. 
 * Allowed values of {@link Compatibility} mode with custom type are AllowAny or DisallowAll.
 */

@Data
@Builder
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

    private static class SchemaTypeBuilder implements ObjectBuilder<SchemaType> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<SchemaType, SchemaType.SchemaTypeBuilder> {
        @Override
        protected SchemaType.SchemaTypeBuilder newBuilder() {
            return SchemaType.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SchemaType e, RevisionDataOutput target) throws IOException {
        }

        private void read00(RevisionDataInput source, SchemaType.SchemaTypeBuilder b) throws IOException {
        }
    }
}
