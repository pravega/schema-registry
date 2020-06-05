/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.schemas;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.storage.impl.group.records.SchemaInfoSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

        private final long fingerprint;

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
                target.writeLong(e.fingerprint);
            }

            private void read00(RevisionDataInput source, SchemaFingerprintKey.SchemaFingerprintKeyBuilder b) throws IOException {
                b.fingerprint(source.readLong());
            }
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    class SchemaIdList implements Value {
        public static final Serializer SERIALIZER = new Serializer();

        private final List<String> schemaIds;

        @SneakyThrows
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
    class SchemaRecord implements Value {
        public static final SchemaRecord.Serializer SERIALIZER = new SchemaRecord.Serializer();

        private final SchemaInfo schemaInfo;

        @Override
        @SneakyThrows
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
                SchemaInfoSerializer.SERIALIZER.serialize(target, e.schemaInfo);
            }

            private void read00(RevisionDataInput source, SchemaRecord.SchemaRecordBuilder b) throws IOException {
                b.schemaInfo(SchemaInfoSerializer.SERIALIZER.deserialize(source));
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

        private final List<String> groupIds;

        @SneakyThrows
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
                target.writeCollection(e.groupIds, DataOutput::writeUTF);
            }

            private void read00(RevisionDataInput source, SchemaGroupsList.SchemaGroupsListBuilder b) throws IOException {
                b.groupIds(Lists.newArrayList(source.readCollection(DataInput::readUTF)));
            }
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Value> T fromBytes(Class<? extends Key> keyClass, byte[] bytes, Class<T> valueClass) {
        return (T) SERIALIZERS_BY_KEY_TYPE.get(keyClass).deserialize(bytes);
    }

    public class KeySerializer extends VersionedSerializer.MultiType<Key> {
        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(SchemaRecords.SchemaIdKey.class, 1, new SchemaRecords.SchemaIdKey.Serializer())
                   .serializer(SchemaRecords.SchemaFingerprintKey.class, 2, new SchemaRecords.SchemaFingerprintKey.Serializer())
                   .serializer(SchemaRecords.SchemaGroupsKey.class, 3, new SchemaRecords.SchemaGroupsKey.Serializer());
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
