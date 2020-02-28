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

/**
 * Encoding Info describes the details of encoding for each event payload. Each combination of schema version and compression type
 * is uniquely identified by an {@link EncodingId}. 
 * The registry service exposes APIs to generate or resolve {@link EncodingId} to {@link EncodingInfo}.
 */
@Data
@Builder
@AllArgsConstructor
public class EncodingInfo {
    public static final Serializer SERIALIZER = new Serializer();

    private final VersionInfo versionInfo;
    private final SchemaInfo schemaInfo;
    private final CompressionType compression;

    private static class EncodingInfoBuilder implements ObjectBuilder<EncodingInfo> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<EncodingInfo, EncodingInfo.EncodingInfoBuilder> {
        @Override
        protected EncodingInfo.EncodingInfoBuilder newBuilder() {
            return EncodingInfo.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(EncodingInfo e, RevisionDataOutput target) throws IOException {
            VersionInfo.SERIALIZER.serialize(target, e.versionInfo);
            SchemaInfo.SERIALIZER.serialize(target, e.schemaInfo);
            CompressionType.SERIALIZER.serialize(target, e.compression);
        }

        private void read00(RevisionDataInput source, EncodingInfo.EncodingInfoBuilder b) throws IOException {
            b.versionInfo(VersionInfo.SERIALIZER.deserialize(source))
             .schemaInfo(SchemaInfo.SERIALIZER.deserialize(source))
             .compression(CompressionType.SERIALIZER.deserialize(source));
        }
    }
}
