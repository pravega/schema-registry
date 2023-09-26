/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.schemas;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;
import io.pravega.schemaregistry.storage.impl.group.records.SerializationFormatRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Records with different implementations for {@link Key} and {@link Value}.
 */
public interface SchemaRecords {
    Map<Class<? extends Key>, ? extends VersionedSerializer.WithBuilder<? extends Value,
            ? extends ObjectBuilder<? extends Value>>> SERIALIZERS_BY_KEY_TYPE =
            ImmutableMap.<Class<? extends Key>, VersionedSerializer.WithBuilder<? extends Value, ? extends ObjectBuilder<? extends Value>>>builder()
                    .put(SchemaIdKey.class, SchemaRecord.SERIALIZER)
                    .put(SchemaFingerprintKey.class, SchemaIdList.SERIALIZER)
                    .put(SchemaGroupsKey.class, SchemaGroupsList.SERIALIZER)
                    .put(SchemaIdChunkKey.class, SchemaChunkRecord.SERIALIZER)
                    .build();

    interface Key {
    }

    interface Value {
        byte[] toBytes();
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaFingerprintKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();

        private final BigInteger fingerprint;

        private static class SchemaFingerprintKeyBuilder implements ObjectBuilder<SchemaFingerprintKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaFingerprintKey, SchemaFingerprintKeyBuilder> {
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
    class SchemaIdList implements Value {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<String> schemaIds;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaIdListBuilder implements ObjectBuilder<SchemaIdList> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaIdList, SchemaIdListBuilder> {
            @Override
            protected SchemaIdList.SchemaIdListBuilder newBuilder() {
                return SchemaIdList.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaIdList e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.schemaIds, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, SchemaIdList.SchemaIdListBuilder b) throws IOException {
                b.schemaIds(new ArrayList<>(source.readCollection(DataInput::readUTF)));
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();

        private final String id;

        private static class SchemaIdKeyBuilder implements ObjectBuilder<SchemaIdKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaIdKey, SchemaIdKeyBuilder> {
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
                target.writeUTF(e.id);
            }

            private void read00(RevisionDataInput source, SchemaIdKey.SchemaIdKeyBuilder b) throws IOException {
                b.id(source.readUTF());
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdChunkKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();

        private final String id;
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
                target.writeUTF(e.id);
                target.writeCompactInt(e.chunkNumber);
            }

            private void read00(RevisionDataInput source, SchemaIdChunkKey.SchemaIdChunkKeyBuilder b) throws IOException {
                b.id(source.readUTF());
                b.chunkNumber(source.readCompactInt());
            }
        }
    }

    class SchemaRecord implements Value {
        public static final SchemaRecord.Serializer SERIALIZER = new SchemaRecord.Serializer();

        @Getter
        private final String type;
        @Getter
        private final SerializationFormat serializationFormat;
        @Getter
        private final ByteArraySegment schemaBinary;
        @Getter
        private final int maxChunkSize;
        @Getter
        private final int numberOfChunks;
        /**
         * Excluded from serialization.
         */
        @Getter
        private final SchemaInfo schemaInfo;

        @Builder
        public SchemaRecord(String type, SerializationFormat serializationFormat, 
                            ByteArraySegment schemaBinary, int maxChunkSize, int numberOfChunks) {
            this.type = type;
            this.serializationFormat = serializationFormat;
            this.schemaBinary = schemaBinary;
            this.maxChunkSize = maxChunkSize;
            this.numberOfChunks = numberOfChunks;
            this.schemaInfo = numberOfChunks == 0 ? new SchemaInfo(type, serializationFormat, 
                    ByteBuffer.wrap(schemaBinary.array(), schemaBinary.arrayOffset(), schemaBinary.getLength()), ImmutableMap.of()) : null;
        }

        public SchemaRecord(SchemaInfo schemaInfo) {
            this.type = schemaInfo.getType();
            this.serializationFormat = schemaInfo.getSerializationFormat();
            this.schemaBinary = new ByteArraySegment(schemaInfo.getSchemaData());
            this.maxChunkSize = schemaBinary.getLength();
            this.numberOfChunks = 0;
            this.schemaInfo = schemaInfo;
        }

        @Override
        @SneakyThrows(IOException.class)
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaRecordBuilder implements ObjectBuilder<SchemaRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaRecord, SchemaRecordBuilder> {
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
                target.writeUTF(e.getType());
                SerializationFormatRecord.SERIALIZER.serialize(target, new SerializationFormatRecord(e.getSerializationFormat()));
                target.writeArray(e.schemaBinary.array(), e.schemaBinary.arrayOffset(), e.schemaBinary.getLength());
                target.writeInt(e.maxChunkSize);
                target.writeCompactInt(e.numberOfChunks);
            }

            private void read00(RevisionDataInput source, SchemaRecord.SchemaRecordBuilder b) throws IOException {
                b.type(source.readUTF())
                 .serializationFormat(SerializationFormatRecord.SERIALIZER.deserialize(source).getSerializationFormat())
                 .schemaBinary(new ByteArraySegment(source.readArray()))
                 .maxChunkSize(source.readInt())
                 .numberOfChunks(source.readCompactInt());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaChunkRecord implements Value {
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
    class SchemaGroupsKey implements Key {
        public static final Serializer SERIALIZER = new Serializer();
        private final String schemaId;

        private static class SchemaGroupsKeyBuilder implements ObjectBuilder<SchemaGroupsKey> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaGroupsKey, SchemaGroupsKeyBuilder> {
            @Override
            protected SchemaGroupsKey.SchemaGroupsKeyBuilder newBuilder() {
                return SchemaGroupsKey.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaGroupsKey e, RevisionDataOutput target) throws IOException {
                target.writeUTF(e.schemaId);
            }

            private void read00(RevisionDataInput source, SchemaGroupsKey.SchemaGroupsKeyBuilder b) throws IOException {
                b.schemaId(source.readUTF());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaGroupsList implements Value {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<NamespaceAndGroup> groupIds;

        @SneakyThrows(IOException.class)
        @Override
        public byte[] toBytes() {
            return SERIALIZER.serialize(this).getCopy();
        }

        private static class SchemaGroupsListBuilder implements ObjectBuilder<SchemaGroupsList> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<SchemaGroupsList, SchemaGroupsListBuilder> {
            @Override
            protected SchemaGroupsList.SchemaGroupsListBuilder newBuilder() {
                return SchemaGroupsList.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SchemaGroupsList e, RevisionDataOutput target) throws IOException {
                target.writeCollection(e.groupIds, NamespaceAndGroup.SERIALIZER::serialize);
            }

            private void read00(RevisionDataInput source, SchemaGroupsList.SchemaGroupsListBuilder b) throws IOException {
                b.groupIds(Lists.newArrayList(source.readCollection(NamespaceAndGroup.SERIALIZER::deserialize)));
            }
        }
    }

    @SneakyThrows(IOException.class)
    @SuppressWarnings("unchecked")
    static <T extends Value> T fromBytes(Class<? extends Key> keyClass, byte[] bytes, Class<T> valueClass) {
        val versionSerializer = SERIALIZERS_BY_KEY_TYPE.get(keyClass);
        if ( versionSerializer == null ) {
            throw new SerializationException(String.format("No serializer found for the class %s", keyClass.toGenericString()));
        }
        return (T) versionSerializer.deserialize(bytes);
    }

    public class KeySerializer extends VersionedSerializer.MultiType<Key> {
        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(SchemaRecords.SchemaIdKey.class, 1, new SchemaRecords.SchemaIdKey.Serializer())
                   .serializer(SchemaRecords.SchemaFingerprintKey.class, 2, new SchemaRecords.SchemaFingerprintKey.Serializer())
                   .serializer(SchemaRecords.SchemaGroupsKey.class, 3, new SchemaRecords.SchemaGroupsKey.Serializer())
                   .serializer(SchemaRecords.SchemaIdChunkKey.class, 4, new SchemaRecords.SchemaIdChunkKey.Serializer());
        }

        /**
         * Serializes the given {@link Key} to a {@link ByteBuffer}.
         *
         * @param value The {@link Key} to serialize.
         * @return An array that contains the serialized key.
         */
        @SneakyThrows(IOException.class)
        public byte[] toBytes(Key value) {
            ByteArraySegment s = serialize(value);
            return s.getCopy();
        }

        /**
         * Deserializes the given buffer into a {@link Key} instance.
         *
         * @param buffer buffer to deserialize into key.
         * @return A new {@link Key} instance from the given serialization.
         */
        @SneakyThrows(IOException.class)
        public Key fromBytes(byte[] buffer) {
            return deserialize(new ByteArraySegment(buffer));
        }
    }
}
