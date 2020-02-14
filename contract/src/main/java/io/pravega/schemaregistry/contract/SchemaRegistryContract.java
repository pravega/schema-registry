/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract;

import com.google.common.collect.ImmutableMap;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public interface SchemaRegistryContract {
    @Data
    class GroupProperties {
        private final SchemaType schemaType;
        private final SchemaValidationRules schemaValidationRules;
        private final boolean subgroupBySchemaName;
        private final boolean enableEncoding;
    }

    @Data
    class SchemaWithVersion {
        private final SchemaInfo schema;
        private final VersionInfo version;
    }

    @Data
    class SchemaEvolution {
        private final SchemaInfo schema;
        private final VersionInfo version;
        private final SchemaValidationRules rules;
    }

    @Data
    class EncodingInfo {
        private final SchemaInfo schemaInfo;
        private final CompressionType compressionType;
    }

    @Data
    class EncodingId {
        private final int id;
    }

    @Data
    class VersionInfo {
        static final VersionInfo NON_EXISTENT = new VersionInfo("", -1);
        private final String schemaName;
        private final int version;
    }

    @Data
    class SchemaInfo {
        private final String name;
        private final SchemaType schemaType;
        private final byte[] schemaData;
        private final ImmutableMap<String, String> properties;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SchemaInfo that = (SchemaInfo) o;
            return Objects.equals(name, that.name) &&
                    schemaType == that.schemaType &&
                    Arrays.equals(schemaData, that.schemaData) &&
                    Objects.equals(properties, that.properties);
        }

        @Override
        public int hashCode() {

            int result = Objects.hash(name, schemaType, properties);
            result = 31 * result + Arrays.hashCode(schemaData);
            return result;
        }
    }

    enum SchemaType {
        None,
        Avro,
        Protobuf,
        Json,
        Custom;

        private AtomicReference<String> customTypeName;

        SchemaType() {
            this.customTypeName = new AtomicReference<>();
        }

        public String getCustomTypeName() {
            return customTypeName.get();
        }

        public void setCustomTypeName(String customTypeName) {
            if (this.equals(SchemaType.Custom)) {
                this.customTypeName.set(customTypeName);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    enum CompressionType {
        None,
        Snappy,
        GZip,
        Custom
    }

    @Data
    public class SchemaValidationRules {
        private final List<Rule> rules;
    }
    
    public interface Rule {
        
    }
}