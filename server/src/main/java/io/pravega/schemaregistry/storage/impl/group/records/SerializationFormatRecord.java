/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Container object for {@link SerializationFormat}.
 */
@Data
@Builder
@AllArgsConstructor
public class SerializationFormatRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final SerializationFormat serializationFormat;


    private static class SerializationFormatRecordBuilder implements ObjectBuilder<SerializationFormatRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<SerializationFormatRecord, SerializationFormatRecordBuilder> {
        @Override
        protected SerializationFormatRecord.SerializationFormatRecordBuilder newBuilder() {
            return SerializationFormatRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SerializationFormatRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.serializationFormat.ordinal());
            if (e.serializationFormat.equals(SerializationFormat.Custom)) {
                target.writeUTF(e.serializationFormat.getCustomTypeName());
            }
        }

        private void read00(RevisionDataInput source, SerializationFormatRecord.SerializationFormatRecordBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            SerializationFormat serializationFormat = SerializationFormat.values()[ordinal];
            if (serializationFormat.equals(SerializationFormat.Custom)) {
                b.serializationFormat(SerializationFormat.custom(source.readUTF()));
            } else {
                b.serializationFormat(serializationFormat);
            }
        }
    }
}
