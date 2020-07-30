/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.json.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractSerializer;
import io.pravega.schemaregistry.serializer.shared.impl.ClosableDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.ClosableSerializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.MultiplexedAndGenericDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.MultiplexedDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.MultiplexedSerializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForSerializer;

/**
 * Internal Factory class for json serializers and deserializers. 
 */
@Slf4j
public class JsonSerializerFactory {
    /**
     * Creates a typed json serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     *
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an Json Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> ClosableSerializer<T> serializer(SerializerConfig config, JSONSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        return new JsonSerializer<>(groupId, schemaRegistryClient, schema, config.getEncoder(),
                config.isRegisterSchema(), config.isWriteEncodingHeader());
    }

    /**
     * Creates a typed json deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an JSONSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use 
     * {@link #genericDeserializer(SerializerConfig)}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> ClosableDeserializer<T> deserializer(SerializerConfig config, JSONSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new JsonDeserializer<>(groupId, schemaRegistryClient, schema, config.getDecoders(), encodingCache, 
                config.isWriteEncodingHeader());
    }

    /**
     * Creates a generic json deserializer.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static ClosableDeserializer<JsonNode> genericDeserializer(SerializerConfig config) {
        Preconditions.checkNotNull(config);
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new JsonGenericDeserializer(groupId, schemaRegistryClient, config.getDecoders(),
                encodingCache, config.isWriteEncodingHeader());
    }

    /**
     * Creates a generic json deserializer which deserializes bytes into a json string.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static ClosableDeserializer<String> deserializeAsString(SerializerConfig config) {
        Preconditions.checkNotNull(config);
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new JsonStringDeserializer(groupId, schemaRegistryClient, config.getDecoders(), encodingCache, config.isWriteEncodingHeader());
    }

    /**
     * A multiplexed Json serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of json schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T> ClosableSerializer<T> multiTypeSerializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        Map<Class<? extends T>, AbstractSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new JsonSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getEncoder(),
                                config.isRegisterSchema(), config.isWriteEncodingHeader())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed json Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of json schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects.
     */
    public static <T> ClosableDeserializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new JsonDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(),
                                encodingCache, config.isWriteEncodingHeader())));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, config.getDecoders(), encodingCache);
    }

    /**
     * A multiplexed json Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of json schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects.
     */
    public static <T> ClosableDeserializer<Either<T, JsonNode>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new JsonDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(), encodingCache, 
                                config.isWriteEncodingHeader())));
        JsonGenericDeserializer genericDeserializer = new JsonGenericDeserializer(groupId, schemaRegistryClient, config.getDecoders(),
                encodingCache, config.isWriteEncodingHeader());

        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, genericDeserializer, config.getDecoders(), encodingCache);
    }
}
