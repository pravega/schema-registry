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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.initForSerializer;

/**
 * Internal Factory class for protobuf serializers and deserializers. 
 */
@Slf4j
class ProtobufSerializerFactory {
    static <T extends Message> Serializer<T> serializer(SerializerConfig config,
                                                        ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        return new ProtobufSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(),
                config.isRegisterSchema());
    }

    static <T extends GeneratedMessageV3> Serializer<T> deserializer(SerializerConfig config,
                                                                     ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new ProtobufDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    static Serializer<DynamicMessage> genericDeserializer(SerializerConfig config, @Nullable ProtobufSchema<DynamicMessage> schema) {
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, schema, config.getDecoder(), encodingCache);
    }

    static <T extends GeneratedMessageV3> Serializer<T> multiTypeSerializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        
        Map<Class<? extends T>, AbstractSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new ProtobufSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getCodec(),
                                config.isRegisterSchema())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    static <T extends GeneratedMessageV3> Serializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new ProtobufDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoder(), encodingCache);
    }

    static <T extends GeneratedMessageV3> Serializer<Either<T, DynamicMessage>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new ProtobufDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        ProtobufGenericDeserlizer genericDeserializer = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null,
                config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoder(), encodingCache);
    }
}
