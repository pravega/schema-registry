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
import com.google.common.base.Strings;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.codec.Codecs;
import io.pravega.schemaregistry.codec.Decoder;
import io.pravega.schemaregistry.codec.Encoder;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer Config class that is passed to {@link SerializerFactory} for creating serializer. 
 */
@Data
@Builder
public class SerializerConfig {
    /**
     * Name of the group.
     */
    @NonNull
    private final String groupId;
    /**
     * Either the registry client or the {@link SchemaRegistryClientConfig} that can be used for creating a new registry client.
     * Exactly one of the two option has to be supplied.
     */
    @Getter(AccessLevel.NONE)
    private final SchemaRegistryClientConfig registryConfig;
    /**
     * Either the registry client or the {@link SchemaRegistryClientConfig} that can be used for creating a new registry client.
     * Exactly one of the two option has to be supplied.
     */
    @Getter(AccessLevel.NONE)
    private final SchemaRegistryClient registryClient;
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
     * Codec to use for encoding events after serializing them.
     */
    private final Encoder encoder;
    /**
     * Function that should be applied on serialized data read from stream. This is invoked after reading the codecType
     * from {@link EncodingInfo} and using the codec type read from it.
     * It should return the decoded data back to the deserializer.
     * Use {@link SerializerConfigBuilder#decoder(String, Decoder)} to add decoders. 
     * Any number of decoders can be added. 
     */
    private final Decoders decoders;
    /**
     * Tells the deserializer that if supplied decoder codecTypes do not match group codecTypes then fail and exit upfront.
     */
    private final boolean failOnCodecMismatch;
    /**
     * Flag to tell the serializer/deserializer if the group should be created automatically.
     * It is recommended to register keep this flag as false in production systems and create groups and add schemas
     */
    @Getter(AccessLevel.NONE)
    private final GroupProperties createGroup;
    /**
     * Flag to tell the serializer/deserializer if the encoding id should be added as a header with each event.
     * By default this is set to true. If users choose to not add the header, they should do so in all their writer and 
     * reader applications for the given stream. 
     *
     * Adding the event header is a requirement for following cases: 
     * If {@link SerializationFormat#Avro} is chosen for a group, the event header cannot be false.
     * If streams can have multiple types of events, this cannot be false.
     * If streams can multiple formats of events, this cannot be false.
     */
    private final boolean writeEncodingHeader;

    private SerializerConfig(String groupId, SchemaRegistryClientConfig config, SchemaRegistryClient client,
                             boolean registerSchema, boolean registerCodec, Codec encoder, Decoders decoders, boolean failOnCodecMismatch,
                             GroupProperties createGroup, boolean writeEncodingHeader) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "Group id needs to be supplied");
        Preconditions.checkArgument(client != null || config != null, "Either registry client or config needs to be supplied");
        this.groupId = groupId;
        this.registryClient = client;
        this.registryConfig = config;
        this.registerSchema = registerSchema;
        this.registerCodec = registerCodec;
        this.encoder = encoder;
        this.decoders = decoders;
        this.failOnCodecMismatch = failOnCodecMismatch;
        this.createGroup = createGroup;
        this.writeEncodingHeader = writeEncodingHeader;
    }

    Either<SchemaRegistryClientConfig, SchemaRegistryClient> getRegistryConfigOrClient() {
        if (registryClient == null) {
            return Either.left(registryConfig);
        } else {
            return Either.right(registryClient);
        }
    }
    
    public boolean isCreateGroup() {
        return createGroup != null;
    }

    GroupProperties getGroupProperties() {
        return createGroup;
    }

    public static final class SerializerConfigBuilder {
        private Codec encoder = Codecs.None.getCodec();

        private Decoders decoders = new Decoders();

        private boolean registerSchema = false;
        private boolean registerCodec = false;
        private boolean failOnCodecMismatch = true;
        private boolean writeEncodingHeader = true;
        private SchemaRegistryClientConfig registryConfig = null;
        private SchemaRegistryClient registryClient = null;
        
        /**
         * Add a decoder for decoding data encoded with the {@link Codec#getCodecType()}. 
         * 
         * @param name Name of codec from {@link CodecType#getName()}.
         * @param decoder decoder implementation to use for decoding data encoded with the {@link Codec#getCodecType()}.
         * @return Builder.
         */
        public SerializerConfigBuilder decoder(String name, Decoder decoder) {
            this.decoders.add(name, decoder);
            return this;
        }

        /**
         * Add multiple decoders. 
         * 
         * @param decoders map of codec name to decoder for the codec. 
         * @return Builder.
         */
        public SerializerConfigBuilder decoders(Map<String, Decoder> decoders) {
            this.decoders.addAll(decoders);
            return this;
        }

        /**
         * Automatically create group with provided group properties values, defaulting compatibility to Full Transitive
         * and allowMultipleTypes to true.
         * Group creation is idempotent.
         *
         * @param serializationFormat {@link GroupProperties#serializationFormat}.
         * @return Builder
         */
        public SerializerConfigBuilder createGroup(SerializationFormat serializationFormat) {
            return createGroup(serializationFormat, true);
        }

        /**
         * Automatically create group with provided group properties values, defaulting compatibility to Full Transitive.
         * Group creation is idempotent.
         *
         * @param serializationFormat {@link GroupProperties#serializationFormat}.
         * @param allowMultipleTypes  {@link GroupProperties#allowMultipleTypes}
         * @return Builder
         */
        public SerializerConfigBuilder createGroup(SerializationFormat serializationFormat, boolean allowMultipleTypes) {
            Compatibility policy = serializationFormat.equals(SerializationFormat.Any) ? Compatibility.allowAny() : 
                    Compatibility.fullTransitive();
            return createGroup(serializationFormat, policy, allowMultipleTypes);
        }

        /**
         * Automatically create group with provided group properties. Group creation is idempotent.
         *
         * @param serializationFormat {@link GroupProperties#serializationFormat}.
         * @param policy              {@link GroupProperties#compatibility}
         * @param allowMultipleTypes  {@link GroupProperties#allowMultipleTypes}
         * @return Builder
         */
        public SerializerConfigBuilder createGroup(SerializationFormat serializationFormat, Compatibility policy, boolean allowMultipleTypes) {
            this.createGroup = new GroupProperties(serializationFormat, policy, allowMultipleTypes);
            return this;
        }

        /**
         * Schema Registry client. Either of client or config should be supplied. 
         *
         * @param client Schema Registry client
         * @return Builder
         */
        public SerializerConfigBuilder registryClient(SchemaRegistryClient client) {
            Preconditions.checkArgument(client != null);
            Preconditions.checkState(registryConfig == null, "Cannot specify both client and config");
            this.registryClient = client;
            return this;
        }

        /**
         * Schema Registry client config which is used to initialize the schema registry client. 
         * Either config or client should be supplied. 
         *
         * @param config Schema Registry client configuration.
         * @return Builder
         */
        public SerializerConfigBuilder registryConfig(SchemaRegistryClientConfig config) {
            Preconditions.checkArgument(config != null);
            Preconditions.checkState(registryClient == null, "Cannot specify both client and config");
            this.registryConfig = config;
            return this;
        }
    }

    static class Decoders {
        private final ConcurrentHashMap<String, Decoder> decoders;

        Decoders() {
            this.decoders = new ConcurrentHashMap<>();
            this.decoders.put(Codecs.None.getCodec().getName(), Codecs.None.getCodec());
            this.decoders.put(Codecs.GzipCompressor.getCodec().getName(), Codecs.GzipCompressor.getCodec());
            this.decoders.put(Codecs.SnappyCompressor.getCodec().getName(), Codecs.SnappyCompressor.getCodec());
        }

        private void add(String codecName, Decoder decoder) {
            Preconditions.checkNotNull(codecName);
            Preconditions.checkNotNull(decoder);
            decoders.put(codecName, decoder);
        }

        private void addAll(Map<String, Decoder> decoders) {
            Preconditions.checkNotNull(decoders);
            this.decoders.putAll(decoders);
        }

        ByteBuffer decode(CodecType codecType, ByteBuffer bytes) throws IOException {
            return decoders.get(codecType.getName()).decode(bytes, codecType.getProperties());
        }

        Set<String> getDecoderNames() {
            return decoders.keySet();  
        }
    }
}
