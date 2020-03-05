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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class CompressionTypeRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final CompressionType compressionType;

    private static class CompressionTypeRecordBuilder implements ObjectBuilder<CompressionTypeRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CompressionTypeRecord, CompressionTypeRecord.CompressionTypeRecordBuilder> {
        @Override
        protected CompressionTypeRecord.CompressionTypeRecordBuilder newBuilder() {
            return CompressionTypeRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CompressionTypeRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.compressionType.ordinal());
            if (e.compressionType.equals(CompressionType.Custom)) {
                target.writeUTF(e.compressionType.getCustomTypeName());
            }
        }

        private void read00(RevisionDataInput source, CompressionTypeRecord.CompressionTypeRecordBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            CompressionType compressionType = CompressionType.values()[ordinal];
            if (compressionType.equals(CompressionType.Custom)) {
                b.compressionType(CompressionType.custom(source.readUTF()));
            } else {
                b.compressionType(compressionType);
            }
        }
    }
}
