/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.CodecType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Container object for {@link CodecType}.
 */
@Data
@Builder
@AllArgsConstructor
public class CodecTypeRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final CodecType codecType;

    private static class CodecTypeRecordBuilder implements ObjectBuilder<CodecTypeRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CodecTypeRecord, CodecTypeRecord.CodecTypeRecordBuilder> {
        @Override
        protected CodecTypeRecord.CodecTypeRecordBuilder newBuilder() {
            return CodecTypeRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CodecTypeRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.codecType.ordinal());
            if (e.codecType.equals(CodecType.Custom)) {
                target.writeUTF(e.codecType.getCustomTypeName());
                target.writeMap(e.codecType.getProperties(), DataOutput::writeUTF, DataOutput::writeUTF);
            }
        }

        private void read00(RevisionDataInput source, CodecTypeRecord.CodecTypeRecordBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            CodecType codecType = CodecType.values()[ordinal];
            if (codecType.equals(CodecType.Custom)) {
                String customTypeName = source.readUTF();
                Map<String, String> properties = source.readMap(DataInput::readUTF, DataInput::readUTF);
                b.codecType(CodecType.custom(customTypeName, properties));
            } else {
                b.codecType(codecType);
            }
        }
    }
}
