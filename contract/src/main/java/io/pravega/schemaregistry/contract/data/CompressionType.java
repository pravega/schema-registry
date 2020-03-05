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
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Different types of compressions used for compressing data while writing it to the stream. 
 * A compression type and schema version combination uniquely identifies encoding format for the serialized data.
 * If a custom compression type which is not identified by the enum is desired by the application, it can be specified using
 * {@link Type#custom} with {@link CompressionType#customTypeName}.  
 */
@Data
@Builder
public class CompressionType {
    public static final Serializer SERIALIZER = new Serializer();
    public static final CompressionType NONE = CompressionType.of(Type.None); 
    public static final CompressionType SNAPPY = CompressionType.of(Type.Snappy); 
    public static final CompressionType GZIP = CompressionType.of(Type.GZip);
    
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
        this.customTypeName = customTypeName == null ? "" : customTypeName;
    }

    public static CompressionType of(Type type) {
        return new CompressionType(type, null);
    }

    public static CompressionType custom(String customTypeName) {
        Preconditions.checkNotNull(customTypeName);
        return new CompressionType(Type.Custom, customTypeName);
    }

    private static class CompressionTypeBuilder implements ObjectBuilder<CompressionType> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CompressionType, CompressionType.CompressionTypeBuilder> {
        @Override
        protected CompressionType.CompressionTypeBuilder newBuilder() {
            return CompressionType.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CompressionType e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.compressionType.ordinal());
            target.writeUTF(e.customTypeName);
        }

        private void read00(RevisionDataInput source, CompressionType.CompressionTypeBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            b.compressionType(Type.values()[ordinal]);
            b.customTypeName(source.readUTF());
        }
    }
}
