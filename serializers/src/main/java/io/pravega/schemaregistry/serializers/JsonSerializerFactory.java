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

import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.schemas.JSONSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.*;

/**
 * Internal Factory class for json serializers and deserializers. 
 */
@Slf4j
class JsonSerializerFactory {
    static <T> Serializer<T> serializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return new JsonSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(),
                config.isRegisterSchema());
    }

    static <T> Serializer<T> deserializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new JsonDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    static Serializer<WithSchema<Object>> genericDeserializer(SerializerConfig config) {
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new JsonGenericDeserializer(groupId, schemaRegistryClient, config.getDecoder(),
                encodingCache);
    }

    static Serializer<String> jsonStringDeserializer(SerializerConfig config) {
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new JsonStringDeserializer(groupId, schemaRegistryClient, config.getDecoder(), encodingCache);
    }

    static <T> Serializer<T> multiTypeSerializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new JsonSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getCodec(),
                                config.isRegisterSchema())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    static <T> Serializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new JsonDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(),
                                encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, config.getDecoder(), encodingCache);
    }

    static <T> Serializer<Either<T, WithSchema<Object>>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new JsonDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        JsonGenericDeserializer genericDeserializer = new JsonGenericDeserializer(groupId, schemaRegistryClient, config.getDecoder(),
                encodingCache);

        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, genericDeserializer, config.getDecoder(), encodingCache);
    }
}
