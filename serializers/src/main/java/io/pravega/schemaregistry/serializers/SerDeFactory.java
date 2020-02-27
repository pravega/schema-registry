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

import com.google.protobuf.Message;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.avro.AvroDeserlizer;
import io.pravega.schemaregistry.serializers.avro.AvroSerializer;
import io.pravega.schemaregistry.serializers.protobuf.ProtobufDeserlizer;
import io.pravega.schemaregistry.serializers.protobuf.ProtobufSerializer;

import java.util.Map;
import java.util.stream.Collectors;

public class SerDeFactory {
    private final SchemaRegistryClient registryClient;

    private SerDeFactory(SchemaRegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    /**
     * Create a simple Avro serializer and Deserializer using the supplied AvroSchema. 
     * This serializer optionally encodes data on the stream  
     * 
     * @param config Serializer config
     * @param schemaData avro schema to use in the serializer and deserializer
     * @param <T> Type of object to be serialized and deserialized into. 
     * @return returns a avro based PravegaSerDe. 
     */
    public <T> PravegaSerDe<T> createAvroSerDe(SerDeConfig config, AvroSchema<T> schemaData) {
        EncodingCache encodingCache = new EncodingCache(config.getScope(), config.getStream(), registryClient);
        AbstractPravegaSerializer<T> serializer = null;
        AbstractPravegaDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            serializer = new AvroSerializer<>(config.getScope(), config.getStream(),
                    registryClient, schemaData, config.getSerializerConfig().getCompressor(), 
                    config.getSerializerConfig().isAutoRegisterSchema(), encodingCache);
        } 
        
        if (config.getDeserializerConfig() != null) {
            deserializer = new AvroDeserlizer<>(config.getScope(), config.getStream(),
                    registryClient, schemaData, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }
        
        return new PravegaSerDe<>(serializer, deserializer);
    }

    public <T> PravegaSerDe<T> createMultiplexedAvroSerDe(SerDeConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        EncodingCache encodingCache = new EncodingCache(config.getScope(), config.getStream(), registryClient);

        MultiplexedSerializer<T> serializer = null;
        MultiplexedDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, 
                            x -> new AvroSerializer<>(config.getScope(), config.getStream(), 
                                    registryClient, x.getValue(), config.getSerializerConfig().getCompressor(),
                                    config.getSerializerConfig().isAutoRegisterSchema(), encodingCache)));
            serializer = new MultiplexedSerializer<>(serializerMap);
        } 
        
        if (config.getDeserializerConfig() != null) {
            Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                            x -> new AvroDeserlizer<>(config.getScope(), config.getStream(),
                                    registryClient, x.getValue(), config.getDeserializerConfig().getCompressorMap(), encodingCache)));
            deserializer = new MultiplexedDeserializer<>(config.getScope(), config.getStream(), registryClient, 
                    deserializerMap, false, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }
        
        return new PravegaSerDe<>(serializer, deserializer);
    }

    public <T extends Message> PravegaSerDe<T> createProtobufSerDe(SerDeConfig config, ProtobufSchema<T> schemaData) {
        EncodingCache encodingCache = new EncodingCache(config.getScope(), config.getStream(), registryClient);

        AbstractPravegaSerializer<T> serializer = null;
        AbstractPravegaDeserializer<T> deserializer = null;
        if (config.getSerializerConfig() != null) {
            // schema cannot be null
            serializer = new ProtobufSerializer<>(config.getScope(), config.getStream(), registryClient, 
                    schemaData, config.getSerializerConfig().getCompressor(),
                    config.getSerializerConfig().isAutoRegisterSchema(), encodingCache);
        }

        if (config.getDeserializerConfig() != null) {
            // schema can be null in which case deserialization will happen into dynamic message
            deserializer = new ProtobufDeserlizer<>(config.getScope(), config.getStream(), registryClient, 
                    schemaData, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }

    public <T extends Message> PravegaSerDe<T> createMultiplexedProtobufSerDe(SerDeConfig config, Map<Class<? extends T>, 
            ProtobufSchema<T>> schemas) {
        EncodingCache encodingCache = new EncodingCache(config.getScope(), config.getStream(), registryClient);

        MultiplexedSerializer<T> serializer = null;
        MultiplexedDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            x -> new ProtobufSerializer<>(config.getScope(), config.getStream(),
                                    registryClient, x.getValue(), config.getSerializerConfig().getCompressor(),
                                    config.getSerializerConfig().isAutoRegisterSchema(), encodingCache)));
            serializer = new MultiplexedSerializer<>(serializerMap);
        }

        if (config.getDeserializerConfig() != null) {
            Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                            x -> new ProtobufDeserlizer<>(config.getScope(), config.getStream(),
                                    registryClient, x.getValue(), config.getDeserializerConfig().getCompressorMap(), encodingCache)));
            deserializer = new MultiplexedDeserializer<>(config.getScope(), config.getStream(), registryClient, 
                    deserializerMap, false, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }
}
