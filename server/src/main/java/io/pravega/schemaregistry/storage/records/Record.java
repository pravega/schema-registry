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
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

public interface Record {
    @Data
    @Builder
    @AllArgsConstructor
    public class SchemaRecord implements Record {
        private final SchemaInfo schemaInfo;
        private final VersionInfo versionInfo;

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
            }

            private void read00(RevisionDataInput source, SchemaRecord.SchemaRecordBuilder b) throws IOException {
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    public class EncodingRecord implements Record {
        private final EncodingId encodingId;
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;

        private static class EncodingRecordBuilder implements ObjectBuilder<EncodingRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<EncodingRecord, EncodingRecord.EncodingRecordBuilder> {
            @Override
            protected EncodingRecord.EncodingRecordBuilder newBuilder() {
                return EncodingRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(EncodingRecord e, RevisionDataOutput target) throws IOException {
            }

            private void read00(RevisionDataInput source, EncodingRecord.EncodingRecordBuilder b) throws IOException {
            }
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    public class ValidationRecord implements Record {
        private final SchemaValidationRules validationRules;
        private static class ValidationRecordBuilder implements ObjectBuilder<ValidationRecord> {
        }

        static class Serializer extends VersionedSerializer.WithBuilder<ValidationRecord, ValidationRecord.ValidationRecordBuilder> {
            @Override
            protected ValidationRecord.ValidationRecordBuilder newBuilder() {
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
            }

            private void read00(RevisionDataInput source, ValidationRecord.ValidationRecordBuilder b) throws IOException {
            }
        }
    }
}
