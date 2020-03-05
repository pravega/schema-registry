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

import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.CompressionType;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

@Data
@Builder
public class SerializerConfig {
    private final String groupId;
    private final Either<SchemaRegistryClientConfig, SchemaRegistryClient> registryConfigOrClient;
    private final boolean autoRegisterSchema;
    private final Compressor compressor;
    private final BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress;
    
    public static final class SerializerConfigBuilder {
        private Compressor compressor = new Compressor.Noop();
        private BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress = (x, y) -> {
            if (x.equals(CompressionType.None)) {
                return y;
            } else {
                throw new IllegalArgumentException();
            }
        };
        private boolean autoRegisterSchema = false;
    }
}
