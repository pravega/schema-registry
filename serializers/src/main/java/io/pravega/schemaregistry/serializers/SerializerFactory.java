/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.google.common.base.Preconditions;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SerializerFactory {
    public static final String ENCODE = "encode";

    // region avro

    /**
     * Creates a typed avro serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     * 
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an AvroSchema
     * @param <T>        Type of event. It accepts either POJO or Avro generated classes and serializes them.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> avroSerializer(SerializerConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        String groupId = config.getGroupId();
        return new AvroSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(), config.isAutoRegisterSchema());
    }

    /**
     * Creates a typed avro deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an AvroSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #avroGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends IndexedRecord> Serializer<T> avroDeserializer(SerializerConfig config,
                                                                           AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        return new AvroDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    /**
     * Creates a generic avro deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link GenericRecord}.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an AvroSchema. It can be null to indicate that writer schema should
     *                   be used for deserialization.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static Serializer<GenericRecord> avroGenericDeserializer(SerializerConfig config,
                                                                    @Nullable AvroSchema<GenericRecord> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);
        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        return new AvroGenericDeserlizer(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Avro serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of avro schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T extends IndexedRecord> Serializer<T> avroMultiTypeSerializer(SerializerConfig config,
                                                                                  Map<Class<? extends T>, AvroSchema<T>> schemas) {
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
                                config.isAutoRegisterSchema())));
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
    public static <T extends SpecificRecordBase> Serializer<T> avroMultiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new AvroDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoder(),
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
    public static <T extends SpecificRecordBase> Serializer<Either<T, GenericRecord>> avroTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new AvroDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        AbstractPravegaDeserializer<GenericRecord> genericDeserializer = new AvroGenericDeserlizer(groupId, schemaRegistryClient,
                null, config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoder(), encodingCache);
    }
    // endregion

    // region protobuf

    /**
     * Creates a typed protobuf serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     * 
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an Protobuf Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T extends Message> Serializer<T> protobufSerializer(SerializerConfig config,
                                                                       ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return new ProtobufSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(),
                config.isAutoRegisterSchema());
    }

    /**
     * Creates a typed protobuf deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an ProtobufSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #protobufGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends GeneratedMessageV3> Serializer<T> protobufDeserializer(SerializerConfig config,
                                                                                    ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new ProtobufDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
    }

    /**
     * Creates a generic protobuf deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link DynamicMessage}.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer.
     * @param schema Schema data that encapsulates an ProtobufSchema.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static Serializer<DynamicMessage> protobufGenericDeserializer(SerializerConfig config, ProtobufSchema<DynamicMessage> schema) {
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        return new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, schema, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Protobuf serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of protobuf schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T extends GeneratedMessageV3> Serializer<T> protobufMultiTypeSerializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new ProtobufSerializer<>(groupId, schemaRegistryClient, x.getValue(), config.getCodec(),
                                config.isAutoRegisterSchema())));
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
    public static <T extends GeneratedMessageV3> Serializer<T> protobufMultiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new ProtobufDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient, deserializerMap, config.getDecoder(), encodingCache);
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
    public static <T extends GeneratedMessageV3> Serializer<Either<T, DynamicMessage>> protobufTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new ProtobufDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        ProtobufGenericDeserlizer genericDeserializer = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null,
                config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient, deserializerMap, genericDeserializer,
                config.getDecoder(), encodingCache);
    }
    //endregion

    // region json

    /**
     * Creates a typed json serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     * 
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an Json Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> jsonSerializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return new JsonSerializer<>(groupId, schemaRegistryClient, schemaData, config.getCodec(),
                config.isAutoRegisterSchema());
    }

    /**
     * Creates a typed json deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaData Schema data that encapsulates an JSONSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #jsonGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> Serializer<T> jsonDeserializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new JsonDeserlizer<>(groupId, schemaRegistryClient, schemaData, config.getDecoder(), encodingCache);
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
    public static Serializer<JSonGenericObject> jsonGenericDeserializer(SerializerConfig config) {
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        return new JsonGenericDeserlizer(groupId, schemaRegistryClient, config.getDecoder(),
                encodingCache);
    }

    /**
     * A multiplexed Json serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of json schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T> Serializer<T> jsonMultiTypeSerializer(
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
                                config.isAutoRegisterSchema())));
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
    public static <T> Serializer<T> jsonMultiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new JsonDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(),
                                encodingCache)));
        return new MultiplexedDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, config.getDecoder(), encodingCache);
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
    public static <T> Serializer<Either<T, JSonGenericObject>> jsonTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .values().stream().collect(Collectors.toMap(x -> x.getSchemaInfo().getName(),
                        x -> new JsonDeserlizer<>(groupId, schemaRegistryClient, x, config.getDecoder(), encodingCache)));
        JsonGenericDeserlizer genericDeserializer = new JsonGenericDeserlizer(groupId, schemaRegistryClient, config.getDecoder(),
                encodingCache);

        return new MultiplexedAndGenericDeserializer<>(groupId, schemaRegistryClient,
                deserializerMap, genericDeserializer, config.getDecoder(), encodingCache);
    }
    //endregion

    // region custom

    /**
     * A serializer that uses user supplied implementation of {@link PravegaSerializer} for serializing the objects.
     * It also takes user supplied schema and registers/validates it against the registry.
     *
     * @param config     Serializer config.
     * @param schema     Schema for the object to serialize
     * @param serializer user supplied serializer
     * @param <T>        Type of object to serialize
     * @return Serializer that uses user supplied serialization function for serializing events.
     */
    public static <T> Serializer<T> customSerializer(SerializerConfig config, SchemaContainer<T> schema, PravegaSerializer<T> serializer) {

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return new AbstractPravegaSerializer<T>(groupId, schemaRegistryClient,
                schema, config.getCodec(), config.isAutoRegisterSchema()) {
            @Override
            protected void serialize(T var, SchemaInfo schema, OutputStream outputStream) {
                serializer.serialize(var, schema, outputStream);
            }
        };
    }

    /**
     * A deserializer that uses user supplied implementation of {@link PravegaDeserializer} for deserializing the data into
     * typed java objects.
     *
     * @param config       Serializer config.
     * @param schema       optional Schema for the object to deserialize
     * @param deserializer user supplied deserializer
     * @param <T>          Type of object to deserialize
     * @return Deserializer that uses user supplied deserialization function for deserializing payload into typed events.
     */
    public static <T> Serializer<T> customDeserializer(SerializerConfig config, @Nullable SchemaContainer<T> schema,
                                                       PravegaDeserializer<T> deserializer) {

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        return new AbstractPravegaDeserializer<T>(groupId, schemaRegistryClient, schema, false,
                config.getDecoder(), encodingCache) {
            @Override
            protected T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                return deserializer.deserialize(inputStream, writerSchema, readerSchema);
            }
        };
    }
    // endregion

    // region multi format deserializer

    /**
     * A deserializer that can read data where each event could be written with different serialization formats.
     *
     * @param config serializer config
     * @return a deserializer that can deserialize protobuf, json or avro events into java objects.
     */
    public static Serializer<Object> multiFormatGenericDeserializer(SerializerConfig config) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config); 
        failOnCodecMismatch(schemaRegistryClient, config); 
        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        AbstractPravegaDeserializer json = new JsonGenericDeserlizer(config.getGroupId(), schemaRegistryClient,
                config.getDecoder(), encodingCache);
        AbstractPravegaDeserializer protobuf = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);
        AbstractPravegaDeserializer avro = new AvroGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);

        Map<SchemaType, AbstractPravegaDeserializer> map = new HashMap<>();
        map.put(SchemaType.Json, json);
        map.put(SchemaType.Avro, avro);
        map.put(SchemaType.Protobuf, protobuf);
        return new MultipleFormatGenericDeserializer(groupId, schemaRegistryClient, map, config.getDecoder(),
                encodingCache);
    }

    /**
     * A deserializer that can read data where each event could be written with different serialization formats and 
     * deserializes and converts them to a json string.
     *
     * @param config serializer config
     * @return a deserializer that can deserialize protobuf, json or avro events into java objects.
     */
    public static Serializer<String> deserializerAsJsonString(SerializerConfig config) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);
        EncodingCache encodingCache = EncodingCache.getEncodingCacheForGroup(groupId, schemaRegistryClient);

        AbstractPravegaDeserializer json = new JsonGenericDeserlizer(config.getGroupId(), schemaRegistryClient,
                config.getDecoder(), encodingCache);
        AbstractPravegaDeserializer protobuf = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);
        AbstractPravegaDeserializer avro = new AvroGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);

        Map<SchemaType, AbstractPravegaDeserializer> map = new HashMap<>();
        map.put(SchemaType.Json, json);
        map.put(SchemaType.Avro, avro);
        map.put(SchemaType.Protobuf, protobuf);
        return new MultipleFormatJsonStringDeserializer(groupId, schemaRegistryClient, map, config.getDecoder(),
                encodingCache);
    }
    // endregion

    private static void autoCreateGroup(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isAutoCreateGroup()) {
            client.addGroup(config.getGroupId(), config.getGroupProperties().getSchemaType(),
                    config.getGroupProperties().getSchemaValidationRules(), config.getGroupProperties().isVersionedBySchemaName(),
                    config.getGroupProperties().getProperties());
        }
    }

    private static void registerCodec(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isAutoRegisterCodec()) {
            client.addCodecType(config.getGroupId(), config.getCodec().getCodecType());
        }
    }

    private static void failOnCodecMismatch(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isFailOnCodecMismatch()) {
            List<CodecType> codecsInGroup = client.getCodecTypes(config.getGroupId());
            if (!config.getDecoder().getCodecs().containsAll(codecsInGroup)) {
                log.warn("Not all Codecs are supported by reader. Required codecs = {}", codecsInGroup);
                throw new RuntimeException(String.format("Need all codecs in %s", codecsInGroup.toString()));
            }
        }
    }
}
