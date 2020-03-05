/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import io.pravega.schemaregistry.GroupIdGenerator;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

@Data
@Builder
public class SerDeConfig {
    private final SchemaType schemaType;
    private final String groupId;
    private final boolean autoRegisterSchema;
    private final SerializerConfig serializerConfig;
    private final DeserializerConfig deserializerConfig;
    
    @Data
    @Builder
    public static class SerializerConfig {
        private final Compressor compressor;

        public static final class SerializerConfigBuilder {
            private Compressor compressor = new Compressor.Noop();
        }
    }
    
    @Data
    @Builder
    public static class DeserializerConfig {
        private final Map<CompressionType, Compressor> compressorMap;

        public static final class DeserializerConfigBuilder {
            private Map<CompressionType, Compressor> compressorMap = Collections.singletonMap(
                    CompressionType.NONE, new Compressor.Noop());
        }
    }
    
    public static final class SerDeConfigBuilder {
        private GroupIdGenerator.Type groupIdGeneratorType = GroupIdGenerator.Type.QualifiedStreamName;
        private SerializerConfig serializerConfig = SerializerConfig.builder().build();
        private DeserializerConfig deserializerConfig = DeserializerConfig.builder().build();
        private boolean autoRegisterSchema = false;
    }
}
