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

import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.client.RegistryClientConfig;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
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

    /**
     * Name of the group. 
     */
    private final String groupId;
    /**
     * Either the registry client or the {@link RegistryClientConfig} that can be used for creating a new registry client.
     * Exactly one of the two option has to be supplied. 
     */
    private final Either<RegistryClientConfig, RegistryClient> registryConfigOrClient;
    /**
     * Flag to tell the serializer if the schema should be automatically registered before using it in {@link io.pravega.client.stream.EventStreamWriter}. 
     * It is recommended to register keep this flag as false in production systems and manage schema evolution explicitly and
     * in lockstep with upgrade of existing pravega client applications. 
     */
    private final boolean autoRegisterSchema;
    /**
     * Flag to tell the serializer if the codec should be automatically registered before using the serializer in 
     * {@link io.pravega.client.stream.EventStreamWriter}. 
     * It is recommended to register keep this flag as false in production systems and manage codecs used by writers explicitly
     * so that readers are aware of encodings used. 
     */
    private final boolean autoRegisterCodec;
    /**
     * Codec to use for compressing events after serializing them. 
     */
    private final Codec codec;
    /**
     * Function that should be applied on serialized data read from stream. This is invoked after reading the {@link CodecType}
     * from {@link EncodingInfo} and using the codec type read from it. 
     * It should return the uncompressed data back to the deserializer. 
     */
    private final Decoder decoder;
    /**
     * Tells the deserializer that if supplied decoder codecs do not match group codecs then fail and exit upfront.  
     */
    private final boolean failOnCodecMismatch;
    
    public static final class SerializerConfigBuilder {
        private Codec codec = NOOP;
        
        private Decoder decoder = new Decoder();
        
        private boolean autoRegisterSchema = false;
        private boolean autoRegisterCodec = false;
        private boolean failOnCodecMismatch = false;
        
        public SerializerConfigBuilder addDecoder(CodecType codecType, Function<ByteBuffer, ByteBuffer> decoder) {
            this.decoder = new Decoder(codecType, decoder);
            return this;
        }
    }
    
    static class Decoder {
        private static final BiFunction<CodecType, ByteBuffer, ByteBuffer> DEFAULT = (x, y) -> {
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
        
        @Getter(AccessLevel.PACKAGE)
        private final Set<CodecType> codecs;
        private final BiFunction<CodecType, ByteBuffer, ByteBuffer> decoder;

        private Decoder(CodecType codecType, Function<ByteBuffer, ByteBuffer> decoder) {
            this.decoder = (x, y) -> {
                if (x.equals(codecType)) {
                    return decoder.apply(y);
                } else {
                    return DEFAULT.apply(x, y);
                }
            };
            codecs = new HashSet<>();
            this.codecs.add(CodecType.None);
            this.codecs.add(CodecType.GZip);
            this.codecs.add(CodecType.Snappy);
            this.codecs.add(codecType);
        }

        private Decoder() {
            this.decoder = DEFAULT;
            codecs = new HashSet<>();
            this.codecs.add(CodecType.None);
            this.codecs.add(CodecType.GZip);
            this.codecs.add(CodecType.Snappy);
        }
        
        ByteBuffer decode(CodecType codecType, ByteBuffer bytes) {
            return decoder.apply(codecType, bytes);
        }
    }
}
