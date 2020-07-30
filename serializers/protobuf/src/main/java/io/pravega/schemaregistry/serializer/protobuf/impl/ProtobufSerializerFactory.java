/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.protobuf.impl;

import com.google.common.base.Preconditions;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForSerializer;

/**
 * Internal Factory class for protobuf serializers and deserializers. 
 */
@Slf4j
public class ProtobufSerializerFactory {
    /**
     * Creates a typed protobuf serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     *
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an Protobuf Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T extends Message> ClosableSerializer<T> serializer(SerializerConfig config,
                                                                       ProtobufSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        return new ProtobufSerializer<>(groupId, schemaRegistryClient, schema, config.getEncoder(),
                config.isRegisterSchema(), config.isWriteEncodingHeader());
    }

    /**
     * Creates a typed protobuf deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an ProtobufSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use 
     * {@link #genericDeserializer(SerializerConfig, ProtobufSchema)}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends GeneratedMessageV3> ClosableDeserializer<T> deserializer(SerializerConfig config,
                                                                                      ProtobufSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new ProtobufDeserializer<>(groupId, schemaRegistryClient, schema, config.getDecoders(), encodingCache,
                config.isWriteEncodingHeader());
    }

    /**
     * Creates a generic protobuf deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link DynamicMessage}.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an ProtobufSchema.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static ClosableDeserializer<DynamicMessage> genericDeserializer(SerializerConfig config, @Nullable ProtobufSchema<DynamicMessage> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(schema != null || config.isWriteEncodingHeader(), 
                "Either read schema should be supplied or events should be tagged with encoding ids.");
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new ProtobufGenericDeserializer(groupId, schemaRegistryClient, schema, config.getDecoders(), encodingCache,
                config.isWriteEncodingHeader());
    }

    /**
     * A multiplexed Protobuf serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of protobuf schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T extends GeneratedMessageV3> ClosableSerializer<T> multiTypeSerializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        
        Map<Class<? extends T>, AbstractSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new ProtobufSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getEncoder(),
                                config.isRegisterSchema(), config.isWriteEncodingHeader())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed protobuf Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of protobuf schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects.
     */
    public static <T extends GeneratedMessageV3> ClosableDeserializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new ProtobufDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(), encodingCache,
                                config.isWriteEncodingHeader())));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoders(), encodingCache);
    }

    /**
     * A multiplexed protobuf Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of protobuf schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects.
     */
    public static <T extends GeneratedMessageV3> ClosableDeserializer<Either<T, DynamicMessage>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new ProtobufDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(), encodingCache, 
                                config.isWriteEncodingHeader())));
        ProtobufGenericDeserializer genericDeserializer = new ProtobufGenericDeserializer(groupId, schemaRegistryClient, null,
                config.getDecoders(), encodingCache, config.isWriteEncodingHeader());
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoders(), encodingCache);
    }
}
