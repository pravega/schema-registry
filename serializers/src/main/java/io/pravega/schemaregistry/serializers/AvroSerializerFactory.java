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
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.*;

/**
 * Internal Factory class for Avro serializers and deserializers. 
 */
@Slf4j
class AvroSerializerFactory {
    static <T> Serializer<T> serializer(SerializerConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        String groupId = config.getGroupId();
        return new AvroSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(), config.isRegisterSchema());
    }

    static <T> Serializer<T> deserializer(SerializerConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AvroDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    static Serializer<Object> genericDeserializer(SerializerConfig config, @Nullable AvroSchema<Object> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AvroGenericDeserlizer(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    static <T> Serializer<T> multiTypeSerializer(SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new AvroSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getCodec(),
                                config.isRegisterSchema())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    static <T> Serializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new AvroDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoder(),
                encodingCache);
    }

    static <T> Serializer<Either<T, Object>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new AvroDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        AbstractPravegaDeserializer<Object> genericDeserializer = new AvroGenericDeserlizer(groupId, schemaRegistryClient,
                null, config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoder(), encodingCache);
    }
}
