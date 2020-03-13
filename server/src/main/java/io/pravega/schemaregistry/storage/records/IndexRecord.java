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
import io.pravega.schemaregistry.contract.data.CompressionTypeRecord;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.Position;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index Records with different implementations for {@link IndexKey} and {@link IndexValue}.
 */
public interface IndexRecord {
    Map<Class<? extends IndexKey>, ? extends VersionedSerializer.WithBuilder<? extends IndexValue, ? extends ObjectBuilder<? extends IndexValue>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends IndexKey>, VersionedSerializer.WithBuilder<? extends IndexValue, ? extends ObjectBuilder<? extends IndexValue>>>builder()
                    .put(VersionInfoKey.class, WALPositionValue.SERIALIZER)
                    .put(SchemaInfoKey.class, SchemaVersionValue.SERIALIZER)
                    .put(GroupPropertyKey.class, WALPositionValue.SERIALIZER)
                    .put(ValidationPolicyKey.class, WALPositionValue.SERIALIZER)
                    .put(SyncdTillKey.class, WALPositionValue.SERIALIZER)
                    .put(EncodingIdIndex.class, EncodingInfoIndex.SERIALIZER)
                    .put(EncodingInfoIndex.class, EncodingIdIndex.SERIALIZER)
                    .build();

    interface IndexKey {
    }
    
    interface IndexValue {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class GroupPropertyKey implements IndexKey {
        public static final Serializer SERIALIZER = new Serializer();
        
        private static class GroupPropertyKeyBuilder implements ObjectBuilder<GroupPropertyKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<GroupPropertyKey, GroupPropertyKey.GroupPropertyKeyBuilder> {
            @Override
            protected GroupPropertyKey.GroupPropertyKeyBuilder newBuilder() {
                return GroupPropertyKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(GroupPropertyKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, GroupPropertyKey.GroupPropertyKeyBuilder b) throws IOException {
                
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ValidationPolicyKey implements IndexKey {
        public static final Serializer SERIALIZER = new Serializer();
        
        private static class ValidationPolicyKeyBuilder implements ObjectBuilder<ValidationPolicyKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationPolicyKey, ValidationPolicyKey.ValidationPolicyKeyBuilder> {
            @Override
            protected ValidationPolicyKey.ValidationPolicyKeyBuilder newBuilder() {
                return ValidationPolicyKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationPolicyKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, ValidationPolicyKey.ValidationPolicyKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SyncdTillKey implements IndexKey {
        public static final Serializer SERIALIZER = new Serializer();
        
        private static class SyncdTillKeyBuilder implements ObjectBuilder<SyncdTillKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SyncdTillKey, SyncdTillKey.SyncdTillKeyBuilder> {
            @Override
            protected SyncdTillKey.SyncdTillKeyBuilder newBuilder() {
                return SyncdTillKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SyncdTillKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, SyncdTillKey.SyncdTillKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaInfoKey implements IndexKey {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final long fingerprint;

        private static class SchemaInfoKeyBuilder implements ObjectBuilder<SchemaInfoKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaInfoKey, SchemaInfoKey.SchemaInfoKeyBuilder> {
            @Override
            protected SchemaInfoKey.SchemaInfoKeyBuilder newBuilder() {
                return SchemaInfoKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaInfoKey e, RevisionDataOutput target) throws IOException {
                target.writeLong(e.fingerprint);
            }

            private void read00(RevisionDataInput source, SchemaInfoKey.SchemaInfoKeyBuilder b) throws IOException {
                b.fingerprint(source.readLong());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionInfoKey implements IndexKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final VersionInfo versionInfo;

        private static class VersionInfoKeyBuilder implements ObjectBuilder<VersionInfoKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionInfoKey, VersionInfoKey.VersionInfoKeyBuilder> {
            @Override
            protected VersionInfoKey.VersionInfoKeyBuilder newBuilder() {
                return VersionInfoKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionInfoKey e, RevisionDataOutput target) throws IOException {
                VersionInfoSerializer.SERIALIZER.serialize(target, e.versionInfo);
            }

            private void read00(RevisionDataInput source, VersionInfoKey.VersionInfoKeyBuilder b) throws IOException {
                b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class WALPositionValue implements IndexValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final Position position;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

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
                assert e.getPosition() instanceof PravegaPosition;
                PravegaPosition.SERIALIZER.serialize(target, (PravegaPosition) e.getPosition());
            }

            private void read00(RevisionDataInput source, WALPositionValue.WALPositionValueBuilder b) throws IOException {
                b.position(PravegaPosition.SERIALIZER.deserialize(source));                
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaVersionValue implements IndexValue {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final List<VersionInfo> versions;
        
        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();    
        }
        
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
                target.writeCollection(e.versions, VersionInfoSerializer.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaVersionValue.SchemaVersionValueBuilder b) throws IOException {
                b.versions(new ArrayList<>(source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingInfoIndex implements IndexKey, IndexValue {
        public static final Serializer SERIALIZER = new Serializer();
        
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;
        
        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

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
                VersionInfoSerializer.SERIALIZER.serialize(target, e.versionInfo);
                CompressionTypeRecord.SERIALIZER.serialize(target, new CompressionTypeRecord(e.compressionType));
            }

            private void read00(RevisionDataInput source, EncodingInfoIndex.EncodingInfoIndexBuilder b) throws IOException {
                b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source))
                 .compressionType(CompressionTypeRecord.SERIALIZER.deserialize(source).getCompressionType());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class EncodingIdIndex implements IndexKey, IndexValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingId encodingId;

        @SneakyThrows
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

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
                EncodingIdSerializer.SERIALIZER.serialize(target, e.encodingId);
            }

            private void read00(RevisionDataInput source, EncodingIdIndex.EncodingIdIndexBuilder b) throws IOException {
                b.encodingId(EncodingIdSerializer.SERIALIZER.deserialize(source));
            }
        }
    }
    
    @SneakyThrows(IOException.class)
    @SuppressWarnings("unchecked")
    static IndexValue fromBytes(Class<? extends IndexKey> clasz, byte[] bytes) {
        return SERIALIZERS_BY_KEY_TYPE.get(clasz).deserialize(bytes);
    }
}
