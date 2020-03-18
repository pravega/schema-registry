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
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import lombok.Builder;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Serializer Config class that is passed to {@link SerializerFactory} for creating serializer. 
 */
@Data
@Builder
public class SerializerConfig {
    private final static Codec NOOP = CodecFactory.none();
    private final static Codec GZIP = CodecFactory.gzip();
    private final static Codec SNAPPY = CodecFactory.snappy();
    private static final BiFunction<CodecType, ByteBuffer, ByteBuffer> DECODER = (x, y) -> {
        switch (x) {
            case None:
                return NOOP.decode(y);
            case GZip:
                return GZIP.decode(y);
            case Snappy:
                return SNAPPY.decode(y);
            default:
                throw new IllegalArgumentException();
        }
    };

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
     * Codec to use for compressing events after serializing them. 
     */
    private final Codec codec;
    /**
     * Function that should be applied on serialized data read from stream. This is invoked after reading the {@link CodecType}
     * from {@link EncodingInfo} and using the codec type read from it. 
     * It should return the uncompressed data back to the deserializer. 
     */
    private final BiFunction<CodecType, ByteBuffer, ByteBuffer> decode;
    
    public static final class SerializerConfigBuilder {
        private Codec codec = NOOP;
        
        private BiFunction<CodecType, ByteBuffer, ByteBuffer> decode = DECODER;
        private boolean autoRegisterSchema = false;
        
        public SerializerConfigBuilder addDecoder(CodecType codecType, Function<ByteBuffer, ByteBuffer> decoder) {
            decode = (x, y) -> {
                if (x.equals(codecType)) {
                    return decoder.apply(y);
                } else {
                    return decode.apply(x, y);
                }
            };
            return this;
        }
    }
}
