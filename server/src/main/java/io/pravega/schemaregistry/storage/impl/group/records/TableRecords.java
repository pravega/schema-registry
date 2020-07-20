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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

/**
 * Index Records with different implementations for {@link TableKey} and {@link TableValue}.
 */
public interface TableRecords {
    Map<Class<? extends TableKey>, ? extends VersionedSerializer.WithBuilder<? extends TableValue,
            ? extends ObjectBuilder<? extends TableValue>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends TableKey>, VersionedSerializer.WithBuilder<? extends TableValue, ? extends ObjectBuilder<? extends TableValue>>>builder()
                    .put(SchemaIdKey.class, SchemaRecord.SERIALIZER)
                    .put(SchemaIdChunkKey.class, SchemaChunkRecord.SERIALIZER)
                    .put(VersionDeletedRecord.class, VersionDeletedRecord.SERIALIZER)
                    .put(SchemaFingerprintKey.class, SchemaVersionList.SERIALIZER)
                    .put(GroupPropertyKey.class, GroupPropertiesRecord.SERIALIZER)
                    .put(ValidationPolicyKey.class, ValidationRecord.SERIALIZER)
                    .put(Etag.class, Etag.SERIALIZER)
                    .put(CodecTypeKey.class, CodecTypeValue.SERIALIZER)
                    .put(CodecTypesKey.class, CodecTypesListValue.SERIALIZER)
                    .put(LatestSchemasKey.class, LatestSchemasValue.SERIALIZER)
                    .put(EncodingIdRecord.class, EncodingInfoRecord.SERIALIZER)
                    .put(EncodingInfoRecord.class, EncodingIdRecord.SERIALIZER)
                    .put(LatestEncodingIdKey.class, LatestEncodingIdValue.SERIALIZER)
                    .put(IndexTypeVersionToIdKey.class, SchemaIdValue.SERIALIZER)
                    .build();

    interface TableKey {
    }

    interface TableValue {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class GroupPropertyKey implements TableKey {
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
    class GroupPropertiesRecord implements TableValue {
        public static final GroupPropertiesRecord.Serializer SERIALIZER = new GroupPropertiesRecord.Serializer();

        private final SerializationFormat serializationFormat;
        private final boolean allowMultipleTypes;
        private final ImmutableMap<String, String> properties;
        
        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class GroupPropertiesRecordBuilder implements ObjectBuilder<GroupPropertiesRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<GroupPropertiesRecord, GroupPropertiesRecord.GroupPropertiesRecordBuilder> {
            @Override
            protected GroupPropertiesRecord.GroupPropertiesRecordBuilder newBuilder() {
                return GroupPropertiesRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(GroupPropertiesRecord e, RevisionDataOutput target) throws IOException {
                SerializationFormatRecord.SERIALIZER.serialize(target, new SerializationFormatRecord(e.serializationFormat));
                target.writeBoolean(e.allowMultipleTypes);
                target.writeMap(e.properties, DataOutput::writeUTF, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, GroupPropertiesRecord.GroupPropertiesRecordBuilder b) throws IOException {
                b.serializationFormat(SerializationFormatRecord.SERIALIZER.deserialize(source).getSerializationFormat())
                 .allowMultipleTypes(source.readBoolean());
                ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.<String, String>builder();
                source.readMap(DataInput::readUTF, DataInput::readUTF, mapBuilder);
                b.properties(mapBuilder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class ValidationPolicyKey implements TableKey {
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
    class ValidationRecord implements TableValue {
        public static final ValidationRecord.Serializer SERIALIZER = new ValidationRecord.Serializer();

        private final Compatibility compatibility;

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class ValidationRecordBuilder implements ObjectBuilder<ValidationRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationRecord, ValidationRecordBuilder> {
            @Override
            protected ValidationRecordBuilder newBuilder() {
                return ValidationRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ValidationRecord e, RevisionDataOutput target) throws IOException {
                CompatibilitySerializer.SERIALIZER.serialize(target, e.compatibility);
            }

            private void read00(RevisionDataInput source, ValidationRecordBuilder b) throws IOException {
                b.compatibility(CompatibilitySerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class Etag implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EtagBuilder implements ObjectBuilder<Etag> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<Etag, Etag.EtagBuilder> {
            @Override
            protected Etag.EtagBuilder newBuilder() {
                return Etag.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(Etag e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, Etag.EtagBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaFingerprintKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final BigInteger fingerprint;

        private static class SchemaFingerprintKeyBuilder implements ObjectBuilder<SchemaFingerprintKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaFingerprintKey, SchemaFingerprintKey.SchemaFingerprintKeyBuilder> {
            @Override
            protected SchemaFingerprintKey.SchemaFingerprintKeyBuilder newBuilder() {
                return SchemaFingerprintKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaFingerprintKey e, RevisionDataOutput target) throws IOException {
                byte[] b = e.fingerprint.toByteArray();
                target.writeArray(b);
            }

            private void read00(RevisionDataInput source, SchemaFingerprintKey.SchemaFingerprintKeyBuilder b) throws IOException {
                b.fingerprint(new BigInteger(source.readArray()));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaVersionList implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final ImmutableList<VersionInfo> versions;
        
        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaVersionListBuilder implements ObjectBuilder<SchemaVersionList> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaVersionList, SchemaVersionList.SchemaVersionListBuilder> {
            @Override
            protected SchemaVersionList.SchemaVersionListBuilder newBuilder() {
                return SchemaVersionList.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaVersionList e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.versions, VersionInfoSerializer.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaVersionList.SchemaVersionListBuilder b) throws IOException {
                ImmutableList.Builder<VersionInfo> builder = ImmutableList.builder();
                source.readCollection(VersionInfoSerializer.SERIALIZER::deserialize, builder);
                b.versions(builder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final int id;

        private static class SchemaIdKeyBuilder implements ObjectBuilder<SchemaIdKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaIdKey, SchemaIdKey.SchemaIdKeyBuilder> {
            @Override
            protected SchemaIdKey.SchemaIdKeyBuilder newBuilder() {
                return SchemaIdKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaIdKey e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.id);
            }

            private void read00(RevisionDataInput source, SchemaIdKey.SchemaIdKeyBuilder b) throws IOException {
                b.id(source.readInt());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdChunkKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final int id;
        private final int chunkNumber;

        private static class SchemaIdChunkKeyBuilder implements ObjectBuilder<SchemaIdChunkKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaIdChunkKey, SchemaIdChunkKey.SchemaIdChunkKeyBuilder> {
            @Override
            protected SchemaIdChunkKey.SchemaIdChunkKeyBuilder newBuilder() {
                return SchemaIdChunkKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaIdChunkKey e, RevisionDataOutput target) throws IOException {
                target.writeCompactInt(e.id);
                target.writeCompactInt(e.chunkNumber);
            }

            private void read00(RevisionDataInput source, SchemaIdChunkKey.SchemaIdChunkKeyBuilder b) throws IOException {
                b.id(source.readCompactInt());
                b.chunkNumber(source.readCompactInt());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class IndexTypeVersionToIdKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final String schemaType;
        private final int version;

        private static class IndexTypeVersionToIdKeyBuilder implements ObjectBuilder<IndexTypeVersionToIdKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<IndexTypeVersionToIdKey, IndexTypeVersionToIdKey.IndexTypeVersionToIdKeyBuilder> {
            @Override
            protected IndexTypeVersionToIdKey.IndexTypeVersionToIdKeyBuilder newBuilder() {
                return IndexTypeVersionToIdKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(IndexTypeVersionToIdKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.schemaType);
                target.writeInt(e.version);
            }

            private void read00(RevisionDataInput source, IndexTypeVersionToIdKey.IndexTypeVersionToIdKeyBuilder b) throws IOException {
                b.schemaType(source.readUTF());
                b.version(source.readInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class VersionDeletedRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final int id;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class VersionDeletedRecordBuilder implements ObjectBuilder<VersionDeletedRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<VersionDeletedRecord, VersionDeletedRecord.VersionDeletedRecordBuilder> {
            @Override
            protected VersionDeletedRecord.VersionDeletedRecordBuilder newBuilder() {
                return VersionDeletedRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(VersionDeletedRecord e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.id);
            }

            private void read00(RevisionDataInput source, VersionDeletedRecord.VersionDeletedRecordBuilder b) throws IOException {
                b.id(source.readInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaRecord implements TableValue {
        public static final SchemaRecord.Serializer SERIALIZER = new SchemaRecord.Serializer();

        private final SchemaInfo schemaInfo;
        private final int id;
        private final int version;
        private final Compatibility compatibility;
        private final long timestamp;
        private final int additionalChunkCount;
        
        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaRecordBuilder implements ObjectBuilder<SchemaRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaRecord, SchemaRecord.SchemaRecordBuilder> {
            @Override
            protected SchemaRecord.SchemaRecordBuilder newBuilder() {
                return SchemaRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaRecord e, RevisionDataOutput target) throws IOException {
                SchemaInfoSerializer.SERIALIZER.serialize(target, e.schemaInfo);
                target.writeCompactInt(e.getId());
                target.writeCompactInt(e.getVersion());
                CompatibilitySerializer.SERIALIZER.serialize(target, e.compatibility);
                target.writeLong(e.timestamp);
                target.writeCompactInt(e.additionalChunkCount);
            }

            private void read00(RevisionDataInput source, SchemaRecord.SchemaRecordBuilder b) throws IOException {
                b.schemaInfo(SchemaInfoSerializer.SERIALIZER.deserialize(source))
                 .id(source.readCompactInt())
                 .version(source.readCompactInt())
                 .compatibility(CompatibilitySerializer.SERIALIZER.deserialize(source))
                 .timestamp(source.readLong())
                 .additionalChunkCount(source.readCompactInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaChunkRecord implements TableValue {
        public static final SchemaChunkRecord.Serializer SERIALIZER = new SchemaChunkRecord.Serializer();

        private final ByteArraySegment chunkPayload;
        
        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaChunkRecordBuilder implements ObjectBuilder<SchemaChunkRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaChunkRecord, SchemaChunkRecord.SchemaChunkRecordBuilder> {
            @Override
            protected SchemaChunkRecord.SchemaChunkRecordBuilder newBuilder() {
                return SchemaChunkRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaChunkRecord e, RevisionDataOutput target) throws IOException {
                target.writeArray(e.chunkPayload.array(), e.chunkPayload.arrayOffset(), e.chunkPayload.getLength());
            }

            private void read00(RevisionDataInput source, SchemaChunkRecord.SchemaChunkRecordBuilder b) throws IOException {
                b.chunkPayload(new ByteArraySegment(source.readArray()));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypesKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class CodecTypesKeyBuilder implements ObjectBuilder<CodecTypesKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypesKey, CodecTypesKey.CodecTypesKeyBuilder> {
            @Override
            protected CodecTypesKey.CodecTypesKeyBuilder newBuilder() {
                return CodecTypesKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypesKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, CodecTypesKey.CodecTypesKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypesListValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final ImmutableList<String> codecTypes;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class CodecTypesListValueBuilder implements ObjectBuilder<CodecTypesListValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypesListValue, CodecTypesListValue.CodecTypesListValueBuilder> {
            @Override
            protected CodecTypesListValue.CodecTypesListValueBuilder newBuilder() {
                return CodecTypesListValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypesListValue e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.codecTypes, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, CodecTypesListValue.CodecTypesListValueBuilder b) throws IOException {
                ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
                source.readCollection(DataInput::readUTF, builder);
                b.codecTypes(builder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypeKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private final String codecTypeName;
        
        private static class CodecTypeKeyBuilder implements ObjectBuilder<CodecTypeKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypeKey, CodecTypeKey.CodecTypeKeyBuilder> {
            @Override
            protected CodecTypeKey.CodecTypeKeyBuilder newBuilder() {
                return CodecTypeKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypeKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.getCodecTypeName());
            }

            private void read00(RevisionDataInput source, CodecTypeKey.CodecTypeKeyBuilder b) throws IOException {
                b.codecTypeName(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class CodecTypeValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final ImmutableMap<String, String> protperties;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class CodecTypeValueBuilder implements ObjectBuilder<CodecTypeValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<CodecTypeValue, CodecTypeValue.CodecTypeValueBuilder> {
            @Override
            protected CodecTypeValue.CodecTypeValueBuilder newBuilder() {
                return CodecTypeValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(CodecTypeValue e, RevisionDataOutput target) throws IOException {
                target.writeMap(e.protperties, DataOutput::writeUTF, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, CodecTypeValue.CodecTypeValueBuilder b) throws IOException {
                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                source.readMap(DataInput::readUTF, DataInput::readUTF, builder);
                b.protperties(builder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemasKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class LatestSchemasKeyBuilder implements ObjectBuilder<LatestSchemasKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestSchemasKey, LatestSchemasKey.LatestSchemasKeyBuilder> {
            @Override
            protected LatestSchemasKey.LatestSchemasKeyBuilder newBuilder() {
                return LatestSchemasKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestSchemasKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, LatestSchemasKey.LatestSchemasKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestSchemasValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final ImmutableMap<String, SchemaTypeValue> types;
        private final int nextId;
        private final ImmutableSet<Integer> deletedIds;
        
        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class LatestSchemasValueBuilder implements ObjectBuilder<LatestSchemasValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestSchemasValue, LatestSchemasValue.LatestSchemasValueBuilder> {
            @Override
            protected LatestSchemasValue.LatestSchemasValueBuilder newBuilder() {
                return LatestSchemasValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestSchemasValue e, RevisionDataOutput target) throws IOException {
                target.writeMap(e.types, DataOutput::writeUTF, SchemaTypeValue.SERIALIZER::serialize);
                target.writeCompactInt(e.nextId);
                target.writeCollection(e.deletedIds, DataOutput::writeInt);
            }

            private void read00(RevisionDataInput source, LatestSchemasValue.LatestSchemasValueBuilder b) throws IOException {
                ImmutableMap.Builder<String, SchemaTypeValue> mapBuilder = ImmutableMap.builder();
                source.readMap(DataInput::readUTF, SchemaTypeValue.SERIALIZER::deserialize, mapBuilder);
                b.types(mapBuilder.build());
                b.nextId(source.readCompactInt());
                ImmutableSet.Builder<Integer> builder = ImmutableSet.<Integer>builder();
                source.readCollection(DataInput::readInt, builder);
                b.deletedIds(builder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaTypeValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final int latestVersion;
        private final int latestId;
        private final int nextVersion;

        private final ImmutableSet<Integer> deletedVersions;
        
        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaTypeValueBuilder implements ObjectBuilder<SchemaTypeValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaTypeValue, SchemaTypeValue.SchemaTypeValueBuilder> {
            @Override
            protected SchemaTypeValue.SchemaTypeValueBuilder newBuilder() {
                return SchemaTypeValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaTypeValue e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.latestVersion);
                target.writeInt(e.latestId);
                target.writeCompactInt(e.nextVersion);
                target.writeCollection(e.deletedVersions, DataOutput::writeInt);
            }

            private void read00(RevisionDataInput source, SchemaTypeValue.SchemaTypeValueBuilder b) throws IOException {
                b.latestVersion(source.readInt())
                 .latestId(source.readInt())
                 .nextVersion(source.readCompactInt());
                ImmutableSet.Builder<Integer> builder = ImmutableSet.<Integer>builder();
                source.readCollection(DataInput::readInt, builder);
                b.deletedVersions(builder.build());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingInfoRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final VersionInfo versionInfo;
        private final String codecType;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EncodingInfoRecordBuilder implements ObjectBuilder<EncodingInfoRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingInfoRecord, EncodingInfoRecord.EncodingInfoRecordBuilder> {
            @Override
            protected EncodingInfoRecord.EncodingInfoRecordBuilder newBuilder() {
                return EncodingInfoRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingInfoRecord e, RevisionDataOutput target) throws IOException {
                VersionInfoSerializer.SERIALIZER.serialize(target, e.versionInfo);
                target.writeUTF(e.codecType);
            }

            private void read00(RevisionDataInput source, EncodingInfoRecord.EncodingInfoRecordBuilder b) throws IOException {
                b.versionInfo(VersionInfoSerializer.SERIALIZER.deserialize(source))
                 .codecType(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class EncodingIdRecord implements TableKey, TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingId encodingId;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class EncodingIdRecordBuilder implements ObjectBuilder<EncodingIdRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingIdRecord, EncodingIdRecord.EncodingIdRecordBuilder> {
            @Override
            protected EncodingIdRecord.EncodingIdRecordBuilder newBuilder() {
                return EncodingIdRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingIdRecord e, RevisionDataOutput target) throws IOException {
                EncodingIdSerializer.SERIALIZER.serialize(target, e.encodingId);
            }

            private void read00(RevisionDataInput source, EncodingIdRecord.EncodingIdRecordBuilder b) throws IOException {
                b.encodingId(EncodingIdSerializer.SERIALIZER.deserialize(source));
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class LatestEncodingIdKey implements TableKey {
        public static final Serializer SERIALIZER = new Serializer();

        private static class LatestEncodingIdKeyBuilder implements ObjectBuilder<LatestEncodingIdKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestEncodingIdKey, LatestEncodingIdKey.LatestEncodingIdKeyBuilder> {
            @Override
            protected LatestEncodingIdKey.LatestEncodingIdKeyBuilder newBuilder() {
                return LatestEncodingIdKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestEncodingIdKey e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, LatestEncodingIdKey.LatestEncodingIdKeyBuilder b) throws IOException {
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class LatestEncodingIdValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final EncodingId encodingId;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class LatestEncodingIdValueBuilder implements ObjectBuilder<LatestEncodingIdValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<LatestEncodingIdValue, LatestEncodingIdValue.LatestEncodingIdValueBuilder> {
            @Override
            protected LatestEncodingIdValue.LatestEncodingIdValueBuilder newBuilder() {
                return LatestEncodingIdValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(LatestEncodingIdValue e, RevisionDataOutput target) throws IOException {
                EncodingIdSerializer.SERIALIZER.serialize(target, e.encodingId);
            }

            private void read00(RevisionDataInput source, LatestEncodingIdValue.LatestEncodingIdValueBuilder b) throws IOException {
                b.encodingId(EncodingIdSerializer.SERIALIZER.deserialize(source));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdValue implements TableValue {
        public static final Serializer SERIALIZER = new Serializer();

        private final int id;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaIdValueBuilder implements ObjectBuilder<SchemaIdValue> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaIdValue, SchemaIdValue.SchemaIdValueBuilder> {
            @Override
            protected SchemaIdValue.SchemaIdValueBuilder newBuilder() {
                return SchemaIdValue.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaIdValue e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.id);
            }

            private void read00(RevisionDataInput source, SchemaIdValue.SchemaIdValueBuilder b) throws IOException {
                b.id(source.readInt());
            }
        }
    }

    @SneakyThrows(IOException.class)
    @SuppressWarnings("unchecked")
    static <T extends TableValue> T fromBytes(Class<? extends TableKey> keyClass, byte[] bytes, Class<T> valueClass) {
        return (T) SERIALIZERS_BY_KEY_TYPE.get(keyClass).deserialize(bytes);
    }
}
