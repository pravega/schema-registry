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
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

/**
 * Serializer Config class that is passed to {@link SerDeFactory} for creating serializer. 
 */
@Data
@Builder
public class SerializerConfig {
    private final static Compressor NOOP = new Compressor.Noop();
    private final static Compressor GZIP = new Compressor.GZipCompressor();
    private final static Compressor SNAPPY = new Compressor.SnappyCompressor();

    /**
     * Name of the group. 
     */
    private final String groupId;
    /**
     * Either the registry client or the {@link SchemaRegistryClientConfig} that can be used for creating a new registry client.
     * Exactly one of the two option has to be supplied. 
     */
    private final Either<SchemaRegistryClientConfig, SchemaRegistryClient> registryConfigOrClient;
    /**
     * Flag to tell the serializer if the schema should be automatically registered before using it in {@link io.pravega.client.stream.EventStreamWriter}. 
     * It is recommended to register keep this flag as false in production systems and manage schema evolution explicitly and
     * in lockstep with upgrade of existing pravega client applications. 
     */
    private final boolean autoRegisterSchema;
    /**
     * Compressor to use for compressing events after serializing them. 
     */
    private final Compressor compressor;
    /**
     * Function that should be applied on serialized data read from stream. This is invoked after reading the {@link CompressionType}
     * from {@link EncodingInfo} and using the compression type read from it. 
     * It should return the uncompressed data back to the deserializer. 
     */
    private final BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress;
    
    public static final class SerializerConfigBuilder {
        private Compressor compressor = NOOP;
        
        private BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress = (x, y) -> {
            switch (x) {
                case None:
                    return NOOP.uncompress(y);
                case GZip:
                    return GZIP.uncompress(y);
                case Snappy:
                    return SNAPPY.uncompress(y);
                default:
                    throw new IllegalArgumentException();
            }
        };
        private boolean autoRegisterSchema = false;
    }
}
