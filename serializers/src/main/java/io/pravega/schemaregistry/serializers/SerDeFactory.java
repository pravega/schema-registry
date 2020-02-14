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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.avro.PravegaAvroDeserlizer;
import io.pravega.schemaregistry.serializers.avro.PravegaAvroSerializer;
import io.pravega.schemaregistry.serializers.protobuf.PravegaProtobufDeserlizer;
import io.pravega.schemaregistry.serializers.protobuf.PravegaProtobufGenericDeserlizer;
import io.pravega.schemaregistry.serializers.protobuf.PravegaProtobufSerializer;
import org.apache.commons.lang.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaType;

public class SerDeFactory {
    private static final Compressor.Noop NOOP = new Compressor.Noop();
    public static final BiFunction<ByteBuffer, CompressionType, ByteBuffer> UNCOMPRESS_LAMBDA = (byteBuffer, compressionType) -> {
        switch (compressionType) {
            case None:
                return NOOP.uncompress(byteBuffer);
            default:
                throw new NotImplementedException();
        }
    };

    private final SchemaRegistryClient registryClient;

    public SerDeFactory(SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
    }
    
    <T> PravegaSerDe<T> createAvroSerDe(String scope, String stream, boolean deserializeIntoWriterSchema) {
        SerializerConfig config = SerializerConfig
                .builder()
                .schemaType(SchemaType.Avro)
                .automaticallyRegisterSchema(false)
                .compressionType(CompressionType.None)
                .deserializeIntoWriterSchema(deserializeIntoWriterSchema)
                .encodeHeader(true)
                .build();
        return createAvroSerDe(scope, stream, null,
                config, new Compressor.Noop());
    }

    <T> PravegaSerDe<T> createAvroSerDe(String scope, String stream,
                                        AvroSchema<T> schemaData, SerializerConfig config, Compressor compressor) {
        Preconditions.checkArgument(config.getSchemaType().equals(SchemaType.Avro));
        Preconditions.checkArgument(config.isEncodeHeader());
        AbstractPravegaSerializer<T> serializer;
        AbstractPravegaDeserializer<T> deserializer;

        serializer = new PravegaAvroSerializer<>(scope, stream, null, registryClient, schemaData, config, compressor);
        
        deserializer = new PravegaAvroDeserlizer<>(scope, stream, registryClient, schemaData, config,
                UNCOMPRESS_LAMBDA);
        
        return new PravegaSerDe<>(serializer, deserializer, registryClient);
    }
    
    PravegaSerDe<DynamicMessage> createProtobufSerDe(String scope, String stream, boolean deserializeIntoWriterSchema, 
                                                     boolean encodeHeader) {
        AbstractPravegaSerializer<DynamicMessage> serializer;
        AbstractPravegaDeserializer<DynamicMessage> deserializer;

        SerializerConfig config = SerializerConfig
                .builder()
                .schemaType(SchemaType.Protobuf)
                .automaticallyRegisterSchema(false)
                .compressionType(CompressionType.None)
                .deserializeIntoWriterSchema(deserializeIntoWriterSchema)
                .encodeHeader(encodeHeader)
                .build();
        
        serializer = new PravegaProtobufSerializer<>(scope, stream, null, registryClient, null,
                config, new Compressor.Noop());

        deserializer = new PravegaProtobufGenericDeserlizer(scope, stream, registryClient, null, config, UNCOMPRESS_LAMBDA);

        return new PravegaSerDe<>(serializer, deserializer, registryClient);
    }

    <T extends Message> PravegaSerDe<T> createProtobufSerDe(String scope, String stream, ProtobufSchema<T> schemaData, 
                                                            SerializerConfig config, Compressor compressor) {
        Preconditions.checkArgument(config.getSchemaType().equals(SchemaType.Protobuf));
        AbstractPravegaSerializer<T> serializer;
        AbstractPravegaDeserializer<T> deserializer;
        serializer = new PravegaProtobufSerializer<>(scope, stream, null, registryClient, schemaData, config, compressor);

        deserializer = new PravegaProtobufDeserlizer<>(scope, stream, registryClient, schemaData, config, UNCOMPRESS_LAMBDA);
        
        return new PravegaSerDe<>(serializer, deserializer, registryClient);
    }
}
