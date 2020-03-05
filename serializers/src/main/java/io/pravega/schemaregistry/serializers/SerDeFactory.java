/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.google.common.base.Preconditions;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.impl.SchemaRegistryClientImpl;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.schemas.SchemaData;
import io.pravega.schemaregistry.serializers.avro.AvroDeserlizer;
import io.pravega.schemaregistry.serializers.avro.AvroGenericDeserlizer;
import io.pravega.schemaregistry.serializers.avro.AvroSerializer;
import io.pravega.schemaregistry.serializers.protobuf.ProtobufDeserlizer;
import io.pravega.schemaregistry.serializers.protobuf.ProtobufGenericDeserlizer;
import io.pravega.schemaregistry.serializers.protobuf.ProtobufSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

public class SerDeFactory {
    public static <T> PravegaSerDe<T> createSerDe(URI registryUri, SerDeConfig config, SchemaData<T> schemaData) {
        SchemaRegistryClientImpl registryClient = new SchemaRegistryClientImpl(registryUri);
        return createSerDe(registryClient, config, schemaData);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> PravegaSerDe<T> createSerDe(SchemaRegistryClient registryClient, SerDeConfig config, SchemaData<T> schemaData) {
        switch (config.getSchemaType().getSchemaType()) {
            case Avro:
                assert schemaData instanceof AvroSchema;
                return avroSerDe(registryClient, config, (AvroSchema<T>) schemaData);
            case Protobuf:
                assert schemaData instanceof ProtobufSchema;
                return (PravegaSerDe<T>) protobufSerDe(registryClient, config, (ProtobufSchema<? extends GeneratedMessageV3>) schemaData);
            default:
                throw new NotImplementedException("serializer does not exist");
        }
    }
    
    public static PravegaSerDe createGenericDeserializer(URI registryUri, SerDeConfig config) {
        SchemaRegistryClientImpl registryClient = new SchemaRegistryClientImpl(registryUri);
        return createGenericDeserializer(registryClient, config);
    }
    
    public static PravegaSerDe createGenericDeserializer(SchemaRegistryClient registryClient, SerDeConfig config) {
        switch (config.getSchemaType().getSchemaType()) {
            case Avro:
                return genericAvroDeserializer(registryClient, config);
            case Protobuf:
                return genericProtobufDeserializer(registryClient, config);
            default:
                throw new NotImplementedException("serializer does not exist");
        }
    }
    
    private static <T> PravegaSerDe<T> avroSerDe(SchemaRegistryClient registryClient, SerDeConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        Preconditions.checkArgument(config.getSchemaType().getSchemaType().equals(SchemaType.Type.Avro));

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);
        AbstractPravegaSerializer<T> serializer = null;
        AbstractPravegaDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            serializer = new AvroSerializer<>(groupId, registryClient, schemaData, config.getSerializerConfig().getCompressor(),
                    config.isAutoRegisterSchema(), encodingCache);
        }

        if (config.getDeserializerConfig() != null) {
            deserializer = new AvroDeserlizer<>(groupId, registryClient, schemaData,
                    config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }

    private static <T extends GeneratedMessageV3> PravegaSerDe<T> protobufSerDe(SchemaRegistryClient registryClient, SerDeConfig config, ProtobufSchema<T> schemaData) {
        Preconditions.checkArgument(config.getSchemaType().getSchemaType().equals(SchemaType.Type.Protobuf));

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        AbstractPravegaSerializer<T> serializer = null;
        AbstractPravegaDeserializer<T> deserializer = null;
        if (config.getSerializerConfig() != null) {
            // schema cannot be null
            serializer = new ProtobufSerializer<>(groupId, registryClient,
                    schemaData, config.getSerializerConfig().getCompressor(),
                    config.isAutoRegisterSchema(), encodingCache);
        }

        if (config.getDeserializerConfig() != null) {
            // schema can be null in which case deserialization will happen into dynamic message
            deserializer = new ProtobufDeserlizer<>(groupId, registryClient,
                    schemaData, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }

    private static PravegaSerDe<GenericRecord> genericAvroDeserializer(SchemaRegistryClient registryClient, SerDeConfig config) {
        Preconditions.checkNotNull(config.getDeserializerConfig());
        Preconditions.checkArgument(config.getSchemaType().getSchemaType().equals(SchemaType.Type.Avro));

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        AbstractPravegaDeserializer<GenericRecord> deserializer = new AvroGenericDeserlizer(groupId, registryClient,
                config.getDeserializerConfig().getCompressorMap(), encodingCache);

        return new PravegaSerDe<>(null, deserializer);
    }

    private static PravegaSerDe<DynamicMessage> genericProtobufDeserializer(SchemaRegistryClient registryClient, SerDeConfig config) {
        Preconditions.checkNotNull(config.getDeserializerConfig());
        Preconditions.checkArgument(config.getSchemaType().getSchemaType().equals(SchemaType.Type.Protobuf));

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        AbstractPravegaDeserializer<DynamicMessage> deserializer = new ProtobufGenericDeserlizer(groupId, registryClient,
                config.getDeserializerConfig().getCompressorMap(), encodingCache);

        return new PravegaSerDe<>(null, deserializer);
    }
    
    public static <T extends GeneratedMessageV3> PravegaSerDe<T> multiplexedProtobufSerDe(SchemaRegistryClient registryClient, SerDeConfig config, 
                                                                                   Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        Preconditions.checkArgument(config.getSchemaType().getSchemaType().equals(SchemaType.Type.Protobuf));

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        MultiplexedSerializer<T> serializer = null;
        MultiplexedDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            x -> new ProtobufSerializer<>(groupId, registryClient, x.getValue(), config.getSerializerConfig().getCompressor(),
                                    config.isAutoRegisterSchema(), encodingCache)));
            serializer = new MultiplexedSerializer<>(serializerMap);
        }

        if (config.getDeserializerConfig() != null) {
            Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                            x -> new ProtobufDeserlizer<>(groupId, registryClient, x.getValue(), config.getDeserializerConfig().getCompressorMap(), encodingCache)));
            deserializer = new MultiplexedDeserializer<>(groupId, registryClient,
                    deserializerMap, false, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }

    public static <T> PravegaSerDe<T> multiplexedAvroSerDe(SchemaRegistryClient registryClient, SerDeConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.getSchemaType().equals(SchemaType.SERIALIZER));

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        MultiplexedSerializer<T> serializer = null;
        MultiplexedDeserializer<T> deserializer = null;

        if (config.getSerializerConfig() != null) {
            Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            x -> new AvroSerializer<>(groupId, registryClient, x.getValue(), config.getSerializerConfig().getCompressor(),
                                    config.isAutoRegisterSchema(), encodingCache)));
            serializer = new MultiplexedSerializer<>(serializerMap);
        }

        if (config.getDeserializerConfig() != null) {
            Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                    .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                            x -> new AvroDeserlizer<>(groupId, registryClient, x.getValue(), config.getDeserializerConfig().getCompressorMap(), encodingCache)));
            deserializer = new MultiplexedDeserializer<>(groupId, registryClient,
                    deserializerMap, false, config.getDeserializerConfig().getCompressorMap(), encodingCache);
        }

        return new PravegaSerDe<>(serializer, deserializer);
    }
}
