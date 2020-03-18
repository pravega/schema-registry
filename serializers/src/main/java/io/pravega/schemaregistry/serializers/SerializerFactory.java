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
import com.google.protobuf.Message;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

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
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an AvroSchema 
     * @param <T> Type of event. It accepts either POJO or Avro generated classes and serializes them. 
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> avroSerializer(SerializerConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);
        return new AvroSerializer<>(groupId, registryClient, schemaData, config.getCodec(),
                config.isAutoRegisterSchema(), encodingCache);
    }

    /**
     * Creates a typed avro deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it. 
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}. 
     * It does not implement {@link Serializer#serialize(Object)}.
     * 
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an AvroSchema 
     * @param <T> Type of event. The typed event should be an avro generated class. For generic type use {@link #genericAvroDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends IndexedRecord> Serializer<T> avroDeserializer(SerializerConfig config, 
                                                                           AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new AvroDeserlizer<>(groupId, registryClient, schemaData, config.getDecoder(), encodingCache);
    }

    /**
     * Creates a generic avro deserializer. It has the optional parameter for schema. 
     * If the schema is not supplied, the writer schema is used for deserialization into {@link GenericRecord}. 
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}. 
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an AvroSchema. It can be null to indicate that writer schema should
     *                   be used for deserialization. 
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static Serializer<GenericRecord> genericAvroDeserializer(SerializerConfig config, 
                                                                    @Nullable AvroSchema<GenericRecord> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new AvroGenericDeserlizer(groupId, registryClient, schemaData, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Avro serializer that takes a map of schemas and validates them individually.
     * 
     * @param config Serializer config. 
     * @param schemas map of avro schemas. 
     * @param <T> Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied. 
     */
    public static <T extends IndexedRecord> Serializer<T> multiTypedAvroSerializer(SerializerConfig config,
                                                                                   Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new AvroSerializer<>(groupId, registryClient, x.getValue(), config.getCodec(),
                                config.isAutoRegisterSchema(), encodingCache)));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of avro schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects. 
     */
    public static <T extends SpecificRecordBase> Serializer<T> multiTypedAvroDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                        x -> new AvroDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, registryClient,
                deserializerMap, false, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Avro Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of avro schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects or a generic 
     * object
     */
    public static <T extends SpecificRecordBase> Serializer<Either<T, GenericRecord>> typedOrGenericAvroDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemas);

        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                        x -> new AvroDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        AbstractPravegaDeserializer<GenericRecord> genericDeserializer = new AvroGenericDeserlizer(groupId, registryClient, 
                null, config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, registryClient,
                deserializerMap, genericDeserializer, false, config.getDecoder(), encodingCache);
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
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an Protobuf Schema. 
     * @param <T> Type of event.  
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T extends Message> Serializer<T> protobufSerializer(SerializerConfig config,
                                                                       ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new ProtobufSerializer<>(groupId, registryClient, schemaData, config.getCodec(),
                config.isAutoRegisterSchema(), encodingCache);
    }
    
    /**
     * Creates a typed protobuf deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it. 
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}. 
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an ProtobufSchema 
     * @param <T> Type of event. The typed event should be an avro generated class. For generic type use {@link #genericProtobufDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends GeneratedMessageV3> Serializer<T> protobufDeserializer(SerializerConfig config, 
                                                                                    ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new ProtobufDeserlizer<>(groupId, registryClient, schemaData, config.getDecoder(), encodingCache);
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
    public static Serializer<DynamicMessage> genericProtobufDeserializer(SerializerConfig config, ProtobufSchema<DynamicMessage> schema) {
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new ProtobufGenericDeserlizer(groupId, registryClient, schema, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Protobuf serializer that takes a map of schemas and validates them individually.
     *
     * @param config Serializer config. 
     * @param schemas map of protobuf schemas. 
     * @param <T> Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied. 
     */
    public static <T extends GeneratedMessageV3> Serializer<T> multiTypedProtobufSerializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new ProtobufSerializer<>(groupId, registryClient, x.getValue(), config.getCodec(),
                                config.isAutoRegisterSchema(), encodingCache)));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed protobuf Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of protobuf schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects. 
     */
    public static <T extends GeneratedMessageV3> Serializer<T> multiTypedProtobufDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                        x -> new ProtobufDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, registryClient,
                deserializerMap, false, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed protobuf Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of protobuf schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects. 
     */
    public static <T extends GeneratedMessageV3> Serializer<Either<T, DynamicMessage>> typedOrGenericProtobufDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                        x -> new ProtobufDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        ProtobufGenericDeserlizer genericDeserializer = new ProtobufGenericDeserlizer(groupId, registryClient, null, config.getDecoder(), encodingCache);
        return new MultiplexedAndGenericDeserializer<>(groupId, registryClient,
                deserializerMap, genericDeserializer, false, config.getDecoder(), encodingCache);
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
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an Json Schema. 
     * @param <T> Type of event.  
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> jsonSerializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new JsonSerializer<>(groupId, registryClient, schemaData, config.getCodec(),
                config.isAutoRegisterSchema(), encodingCache);
    }

    /**
     * Creates a typed json deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it. 
     *
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}. 
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config Serializer Config used for instantiating a new serializer. 
     * @param schemaData Schema data that encapsulates an JSONSchema 
     * @param <T> Type of event. The typed event should be an avro generated class. For generic type use {@link #genericJsonDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> Serializer<T> jsonDeserializer(SerializerConfig config, JSONSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new JsonDeserlizer<>(groupId, registryClient, schemaData, config.getDecoder(), encodingCache);
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
    public static Serializer<JSonGenericObject> genericJsonDeserializer(SerializerConfig config) {
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new JsonGenericDeserlizer(groupId, registryClient, config.getDecoder(), encodingCache);
    }

    /**
     * A multiplexed Json serializer that takes a map of schemas and validates them individually.
     *
     * @param config Serializer config. 
     * @param schemas map of json schemas. 
     * @param <T> Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied. 
     */
    public static <T> Serializer<T> multiTypedJsonSerializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new JsonSerializer<>(groupId, registryClient, x.getValue(), config.getCodec(),
                                config.isAutoRegisterSchema(), encodingCache)));
        return new MultiplexedSerializer<>(serializerMap);
    }

    /**
     * A multiplexed json Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of json schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects. 
     */
    public static <T> Serializer<T> multiTypedJsonDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getSchemaId(),
                        x -> new JsonDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, registryClient,
                deserializerMap, false, config.getDecoder(), encodingCache);
    }
    
    /**
     * A multiplexed json Deserializer that takes a map of schemas and deserializes events into those events depending
     * on the object type information in {@link EncodingInfo}.
     * @param config Serializer config. 
     * @param schemas map of json schemas. 
     * @param <T> Base type of schemas. 
     * @return a Deserializer which can deserialize events of different types in the stream into typed objects. 
     */
    public static <T> Serializer<Either<T, JSonGenericObject>> typedOrGenericJsonDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getValue().getSchemaId(),
                        x -> new JsonDeserlizer<>(groupId, registryClient, x.getValue(), config.getDecoder(), encodingCache)));
        JsonGenericDeserlizer genericDeserializer = new JsonGenericDeserlizer(groupId, registryClient, config.getDecoder(), encodingCache);
        
        return new MultiplexedAndGenericDeserializer<>(groupId, registryClient,
                deserializerMap, genericDeserializer, false, config.getDecoder(), encodingCache);
    }
    //endregion
}
