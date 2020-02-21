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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.Position;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IndexRecord {
    Map<Class<? extends IndexKey>, Class<? extends IndexValue>> ACCEPTED_KEY_VALUES =
            ImmutableMap.<Class<? extends IndexKey>, Class<? extends IndexValue>>builder()
                    .put(VersionInfoKey.class, WALPositionValue.class)
                    .put(SchemaInfoKey.class, SchemaVersionValue.class)
                    .put(ValidationPolicyKey.class, WALPositionValue.class)
                    .put(SyncdTillKey.class, WALPositionValue.class)
                    .put(EncodingIdIndex.class, EncodingInfoIndex.class)
                    .put(EncodingInfoIndex.class, EncodingIdIndex.class)
                    .build();

    interface IndexKey {
        
    }
    
    interface IndexValue {
        
    }

    @Data
    @Builder
    @AllArgsConstructor
    class WALPositionValue implements IndexValue {
        private final Position position;
        private static class WALPositionValueBuilder implements ObjectBuilder<WALPositionValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<WALPositionValue, WALPositionValue.WALPositionValueBuilder> {
            @Override
            protected WALPositionValue.WALPositionValueBuilder newBuilder() {
                return WALPositionValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(WALPositionValue e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, WALPositionValue.WALPositionValueBuilder b) throws IOException {
            }
        }
    }

    @Data
    class SchemaInfoKey implements IndexKey {
        private final long fingerprint;
    }

    @Data
    class VersionInfoKey implements IndexKey {
        private final VersionInfo versionInfo;
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaVersionValue implements IndexValue {
        private final List<VersionInfo> versions;

        private static class SchemaVersionValueBuilder implements ObjectBuilder<SchemaVersionValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaVersionValue, SchemaVersionValue.SchemaVersionValueBuilder> {
            @Override
            protected SchemaVersionValue.SchemaVersionValueBuilder newBuilder() {
                return SchemaVersionValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaVersionValue e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, SchemaVersionValue.SchemaVersionValueBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingInfoIndex implements IndexKey, IndexValue {
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;

        private static class EncodingInfoIndexBuilder implements ObjectBuilder<EncodingInfoIndex> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingInfoIndex, EncodingInfoIndex.EncodingInfoIndexBuilder> {
            @Override
            protected EncodingInfoIndex.EncodingInfoIndexBuilder newBuilder() {
                return EncodingInfoIndex.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingInfoIndex e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, EncodingInfoIndex.EncodingInfoIndexBuilder b) throws IOException {
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class EncodingIdIndex implements IndexKey, IndexValue {
        private final EncodingId encodingId;

        private static class EncodingIdIndexBuilder implements ObjectBuilder<EncodingIdIndex> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingIdIndex, EncodingIdIndex.EncodingIdIndexBuilder> {
            @Override
            protected EncodingIdIndex.EncodingIdIndexBuilder newBuilder() {
                return EncodingIdIndex.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingIdIndex e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, EncodingIdIndex.EncodingIdIndexBuilder b) throws IOException {
            }
        }
    }

    @Data
    class ValidationPolicyKey implements IndexKey {
    }
    
    @Data
    class SyncdTillKey implements IndexKey {
    }
}
