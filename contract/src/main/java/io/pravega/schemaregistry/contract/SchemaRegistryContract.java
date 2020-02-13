/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import lombok.Getter;

import java.util.Map;

public interface SchemaRegistryContract {
    @Data
    class GroupProperties {
        private final SchemaType schemaType;
        private final Compatibility compatibility;
        private final boolean allowSubgroups;
        private final boolean encodeHeader;
    }

    @Data
    class SchemaWithVersion {
        private final SchemaInfo schema;
        private final VersionInfo version;
    }

    @Data
    class EncodingInfo {
        private final SchemaInfo schemaInfo;
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;
    }

    @Data
    class EncodingId {
        private final int id;
    }

    @Data
    class VersionInfo {
        static final VersionInfo NON_EXISTENT = new VersionInfo(-1);
        private final int version;
    }

    @Getter
    class SchemaInfo {
        private final String name;
        private final SchemaType schemaType;
        private final byte[] schemaData;
        private final ImmutableMap<String, String> properties;

        public SchemaInfo(String name, SchemaType schemaType, byte[] schemaData, Map<String, String> properties) {
            this.name = name;
            this.schemaType = schemaType;
            this.schemaData = schemaData;
            this.properties = ImmutableMap.copyOf(properties);
        }
    }

    enum SchemaType {
        None,
        Avro,
        Protobuf,
        Json,
        Custom;

        public String getCustomTypeName() {
            return customTypeName;
        }

        public void setCustomTypeName(String customTypeName) {
            this.customTypeName = customTypeName;
        }

        private String customTypeName;
    }

    enum CompressionType {
        None,
        Snappy,
        GZip,
        Custom
    }
}