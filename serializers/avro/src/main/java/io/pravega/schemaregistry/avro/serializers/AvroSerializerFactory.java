/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.avro.serializers;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.shared.serializers.AbstractDeserializer;
import io.pravega.schemaregistry.shared.serializers.AbstractSerializer;
import io.pravega.schemaregistry.shared.serializers.EncodingCache;
import io.pravega.schemaregistry.shared.serializers.MultiplexedAndGenericDeserializer;
import io.pravega.schemaregistry.shared.serializers.MultiplexedDeserializer;
import io.pravega.schemaregistry.shared.serializers.MultiplexedSerializer;
import io.pravega.schemaregistry.shared.serializers.SerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.shared.serializers.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.shared.serializers.SerializerFactoryHelper.initForSerializer;

/**
 * Internal Factory class for Avro serializers and deserializers. 
 */
@Slf4j
public class AvroSerializerFactory {
    /**
     * Creates a typed avro serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     *
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an AvroSchema
     * @param <T>        Type of event. It accepts either POJO or Avro generated classes and serializes them.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> serializer(SerializerConfig config, AvroSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        String groupId = config.getGroupId();
        return new AvroSerializer<>(groupId, schemaRegistryClient, schema, config.getEncoder(), config.isRegisterSchema());
    }

    /**
     * Creates a typed avro deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an AvroSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use 
     * {@link #genericDeserializer(SerializerConfig, AvroSchema)}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> Serializer<T> deserializer(SerializerConfig config, AvroSchema<T> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schema);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);
        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AvroDeserializer<>(groupId, schemaRegistryClient, schema, config.getDecoders(), encodingCache);
    }

    /**
     * Creates a generic avro deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link GenericRecord}.
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schema Schema container that encapsulates an AvroSchema. It can be null to indicate that writer schema should
     *                   be used for deserialization.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static Serializer<Object> genericDeserializer(SerializerConfig config, @Nullable AvroSchema<Object> schema) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AvroGenericDeserializer(groupId, schemaRegistryClient, schema, config.getDecoders(), encodingCache);
    }

    /**
     * A multiplexed Avro serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of avro schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T> Serializer<T> multiTypeSerializer(SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        Map<Class<? extends T>, AbstractSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new AvroSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getEncoder(),
                                config.isRegisterSchema())));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of avro schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects.
     */
    public static <T> Serializer<T> multiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new AvroDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoders(),
                encodingCache);
    }

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     *
     * @param config  Serializer config.
     * @param schemas map of avro schemas.
     * @param <T>     Base type of schemas.
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects or a generic
     * object
     */
    public static <T> Serializer<Either<T, Object>> typedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        Map<String, AbstractDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getType(),
                        x -> new AvroDeserializer<>(groupId, schemaRegistryClient, x, config.getDecoders(), encodingCache)));
        AbstractDeserializer<Object> genericDeserializer = new AvroGenericDeserializer(groupId, schemaRegistryClient,
                null, config.getDecoders(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoders(), encodingCache);
    }
}
