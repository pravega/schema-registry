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
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Container object for {@link SchemaType}.
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaTypeRecord {
    public static final Serializer SERIALIZER = new Serializer();

    private final SchemaType schemaType;


    private static class SchemaTypeRecordBuilder implements ObjectBuilder<SchemaTypeRecord> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<SchemaTypeRecord, SchemaTypeRecordBuilder> {
        @Override
        protected SchemaTypeRecord.SchemaTypeRecordBuilder newBuilder() {
            return SchemaTypeRecord.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SchemaTypeRecord e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.schemaType.ordinal());
            if (e.schemaType.equals(SchemaType.Custom)) {
                target.writeUTF(e.schemaType.getCustomTypeName());
            }
        }

        private void read00(RevisionDataInput source, SchemaTypeRecord.SchemaTypeRecordBuilder b) throws IOException {
            int ordinal = source.readCompactInt();
            SchemaType schemaType = SchemaType.values()[ordinal];
            if (schemaType.equals(SchemaType.Custom)) {
                b.schemaType(SchemaType.custom(source.readUTF()));
            } else {
                b.schemaType(schemaType);
            }
        }
    }
}
