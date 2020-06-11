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

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.CodecFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.pravega.schemaregistry.codec.CodecFactory.*;

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
     * Either the registry client or the {@link SchemaRegistryClientConfig} that can be used for creating a new registry client.
     * Exactly one of the two option has to be supplied. 
     */
    private final Either<SchemaRegistryClientConfig, SchemaRegistryClient> registryConfigOrClient;
    /**
     * Flag to tell the serializer if the schema should be automatically registered before using it in {@link io.pravega.client.stream.EventStreamWriter}. 
     * It is recommended to register keep this flag as false in production systems and manage schema evolution explicitly and
     * in lockstep with upgrade of existing pravega client applications. 
     */
    private final boolean registerSchema;
    /**
     * Flag to tell the serializer if the codec should be automatically registered before using the serializer in 
     * {@link io.pravega.client.stream.EventStreamWriter}. 
     * It is recommended to register keep this flag as false in production systems and manage codecTypes used by writers explicitly
     * so that readers are aware of encodings used. 
     */
    private final boolean registerCodec;
    /**
     * Codec to use for compressing events after serializing them. 
     */
    private final Codec codec;
    /**
     * Function that should be applied on serialized data read from stream. This is invoked after reading the codecType
     * from {@link EncodingInfo} and using the codec type read from it. 
     * It should return the uncompressed data back to the deserializer. 
     */
    private final Decoder decoder;
    /**
     * Tells the deserializer that if supplied decoder codecTypes do not match group codecTypes then fail and exit upfront.  
     */
    private final boolean failOnCodecMismatch;

    /**
     * Flag to tell the serializer if the group should be created automatically. 
     * It is recommended to register keep this flag as false in production systems and create groups and add schemas 
     */
    private final boolean createGroup;
    /**
     * Group properties to use for creating the group if createGroup is set to true. 
     */
    private final GroupProperties groupProperties;

    public static final class SerializerConfigBuilder {
        private Codec codec = NOOP;

        private Decoder decoder = new Decoder();

        private boolean registerSchema = false;
        private boolean registerCodec = false;
        private boolean createGroup = false;
        private boolean failOnCodecMismatch = true;
        private Either<SchemaRegistryClientConfig, SchemaRegistryClient> registryConfigOrClient = null;

        private GroupProperties groupProperties = GroupProperties.builder().build();

        public SerializerConfigBuilder decoder(String codecType, Function<ByteBuffer, ByteBuffer> decoder) {
            this.decoder = new Decoder(codecType, decoder);
            return this;
        }

        public SerializerConfigBuilder autoCreateGroup(SerializationFormat serializationFormat) {
            return autoCreateGroup(serializationFormat, true);
        }

        public SerializerConfigBuilder autoCreateGroup(SerializationFormat serializationFormat, boolean allowMultipleTypes) {
            return autoCreateGroup(serializationFormat, SchemaValidationRules.of(Compatibility.fullTransitive()), allowMultipleTypes);
        }

        public SerializerConfigBuilder autoCreateGroup(SerializationFormat serializationFormat, SchemaValidationRules rules, boolean allowMultipleTypes) {
            this.createGroup = true;
            this.groupProperties = new GroupProperties(serializationFormat, rules, allowMultipleTypes);
            return this;
        }
        
        public SerializerConfigBuilder registryClient(SchemaRegistryClient client) {
            Preconditions.checkArgument(client != null);
            this.registryConfigOrClient = Either.right(client);
            return this;
        }
        
        public SerializerConfigBuilder registryConfig(SchemaRegistryClientConfig config) {
            Preconditions.checkArgument(config != null);
            this.registryConfigOrClient = Either.left(config);
            return this;
        }
    }

    static class Decoder {
        private static final BiFunction<String, ByteBuffer, ByteBuffer> DEFAULT = (x, y) -> {
            switch (x) {
                case NONE:
                    return NOOP.decode(y);
                case MIME_GZIP:
                    return GZIP.decode(y);
                case MIME_SNAPPY:
                    return SNAPPY.decode(y);
                default:
                    throw new IllegalArgumentException();
            }
        };

        @Getter(AccessLevel.PACKAGE)
        private final Set<String> codecTypes;
        private final BiFunction<String, ByteBuffer, ByteBuffer> decoder;

        private Decoder(String codecType, Function<ByteBuffer, ByteBuffer> decoder) {
            this.decoder = (x, y) -> {
                if (x.equals(codecType)) {
                    return decoder.apply(y);
                } else {
                    return DEFAULT.apply(x, y);
                }
            };
            codecTypes = new HashSet<>();
            this.codecTypes.add(NONE);
            this.codecTypes.add(MIME_GZIP);
            this.codecTypes.add(MIME_SNAPPY);
            this.codecTypes.add(codecType);
        }

        private Decoder() {
            this.decoder = DEFAULT;
            codecTypes = new HashSet<>();
            this.codecTypes.add(NONE);
            this.codecTypes.add(MIME_GZIP);
            this.codecTypes.add(MIME_SNAPPY);
        }

        ByteBuffer decode(String codecType, ByteBuffer bytes) {
            return decoder.apply(codecType, bytes);
        }
    }
}
