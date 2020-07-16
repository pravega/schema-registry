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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.schemas.Schema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;

import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static io.pravega.schemaregistry.serializers.WithSchema.JSON_TRANSFORM;
import static io.pravega.schemaregistry.serializers.WithSchema.NO_TRANSFORM;

@Slf4j
public class RegistrySerializerFactory {
    public static final String PRAVEGA_EVENT_HEADER = "PravegaEventHeader";

    // region avro
    /**
     * Creates a typed avro serializer for the Schema. The serializer implementation returned from this method is
     * responsible for interacting with schema registry service and ensures that only valid registered schema can be used.
     * 
     * Note: the returned serializer only implements {@link Serializer#serialize(Object)}.
     * It does not implement {@link Serializer#deserialize(ByteBuffer)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaContainer Schema container that encapsulates an AvroSchema
     * @param <T>        Type of event. It accepts either POJO or Avro generated classes and serializes them.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> avroSerializer(SerializerConfig config, AvroSchema<T> schemaContainer) {
        return AvroSerializerFactory.serializer(config, schemaContainer);
    }

    /**
     * Creates a typed avro deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaContainer Schema container that encapsulates an AvroSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #avroGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> Serializer<T> avroDeserializer(SerializerConfig config, AvroSchema<T> schemaContainer) {
        return AvroSerializerFactory.deserializer(config, schemaContainer);
    }

    /**
     * Creates a generic avro deserializer. It has the optional parameter for schema.
     * If the schema is not supplied, the writer schema is used for deserialization into {@link GenericRecord}.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaContainer Schema container that encapsulates an AvroSchema. It can be null to indicate that writer schema should
     *                   be used for deserialization.
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static Serializer<Object> avroGenericDeserializer(SerializerConfig config, @Nullable AvroSchema<Object> schemaContainer) {
        return AvroSerializerFactory.genericDeserializer(config, schemaContainer);
    }

    /**
     * A multiplexed Avro serializer that takes a map of schemas and validates them individually.
     *
     * @param config  Serializer config.
     * @param schemas map of avro schemas.
     * @param <T>     Base Type of schemas.
     * @return a Serializer which can serialize events of different types for which schemas are supplied.
     */
    public static <T> Serializer<T> avroMultiTypeSerializer(SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        return AvroSerializerFactory.multiTypeSerializer(config, schemas);
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
    public static <T> Serializer<T> avroMultiTypeDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        return AvroSerializerFactory.multiTypeDeserializer(config, schemas);
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
    public static <T> Serializer<Either<T, Object>> avroTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, AvroSchema<T>> schemas) {
        return AvroSerializerFactory.typedOrGenericDeserializer(config, schemas);
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
     * @param schemaContainer Schema container that encapsulates an Protobuf Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T extends Message> Serializer<T> protobufSerializer(SerializerConfig config,
                                                                       ProtobufSchema<T> schemaContainer) {
        return ProtobufSerializerFactory.serializer(config, schemaContainer);
    }

    /**
     * Creates a typed protobuf deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaContainer Schema container that encapsulates an ProtobufSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #protobufGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T extends GeneratedMessageV3> Serializer<T> protobufDeserializer(SerializerConfig config,
                                                                                    ProtobufSchema<T> schemaContainer) {
        return ProtobufSerializerFactory.deserializer(config, schemaContainer);
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
    public static Serializer<DynamicMessage> protobufGenericDeserializer(SerializerConfig config, 
                                                                         @Nullable ProtobufSchema<DynamicMessage> schema) {
        return ProtobufSerializerFactory.genericDeserializer(config, schema);
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
        return ProtobufSerializerFactory.multiTypeSerializer(config, schemas);
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
        return ProtobufSerializerFactory.multiTypeDeserializer(config, schemas);
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
        return ProtobufSerializerFactory.typedOrGenericDeserializer(config, schemas);
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
     * @param schemaContainer Schema container that encapsulates an Json Schema.
     * @param <T>        Type of event.
     * @return A Serializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamWriter} or
     * {@link io.pravega.client.stream.TransactionalEventStreamWriter}.
     */
    public static <T> Serializer<T> jsonSerializer(SerializerConfig config, JSONSchema<T> schemaContainer) {
        return JsonSerializerFactory.serializer(config, schemaContainer);
    }

    /**
     * Creates a typed json deserializer for the Schema. The deserializer implementation returned from this method is
     * responsible for interacting with schema registry service and validate the writer schema before using it.
     * 
     * Note: the returned serializer only implements {@link Serializer#deserialize(ByteBuffer)}.
     * It does not implement {@link Serializer#serialize(Object)}.
     *
     * @param config     Serializer Config used for instantiating a new serializer.
     * @param schemaContainer Schema container that encapsulates an JSONSchema
     * @param <T>        Type of event. The typed event should be an avro generated class. For generic type use {@link #jsonGenericDeserializer}
     * @return A deserializer Implementation that can be used in {@link io.pravega.client.stream.EventStreamReader}.
     */
    public static <T> Serializer<T> jsonDeserializer(SerializerConfig config, JSONSchema<T> schemaContainer) {
        return JsonSerializerFactory.deserializer(config, schemaContainer);
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
    public static Serializer<WithSchema<JsonNode>> jsonGenericDeserializer(SerializerConfig config) {
        return JsonSerializerFactory.genericDeserializer(config);
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
    public static Serializer<String> jsonStringDeserializer(SerializerConfig config) {
        return JsonSerializerFactory.jsonStringDeserializer(config);
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
        return JsonSerializerFactory.multiTypeSerializer(config, schemas);
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
        return JsonSerializerFactory.multiTypeDeserializer(config, schemas);
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
    public static <T> Serializer<Either<T, WithSchema<JsonNode>>> jsonTypedOrGenericDeserializer(
            SerializerConfig config, Map<Class<? extends T>, JSONSchema<T>> schemas) {
        return JsonSerializerFactory.typedOrGenericDeserializer(config, schemas);
    }
    //endregion

    // region custom

    /**
     * A serializer that uses user supplied implementation of {@link CustomSerializer} for serializing the objects.
     * It also takes user supplied schema and registers/validates it against the registry.
     *
     * @param config     Serializer config.
     * @param schema     Schema for the object to serialize
     * @param serializer user supplied serializer
     * @param <T>        Type of object to serialize
     * @return Serializer that uses user supplied serialization function for serializing events.
     */
    public static <T> Serializer<T> customSerializer(SerializerConfig config, Schema<T> schema, CustomSerializer<T> serializer) {
        return CustomSerializerFactory.serializer(config, schema, serializer);
    }

    /**
     * A deserializer that uses user supplied implementation of {@link CustomDeserializer} for deserializing the data into
     * typed java objects.
     *
     * @param config       Serializer config.
     * @param schema       optional Schema for the object to deserialize
     * @param deserializer user supplied deserializer
     * @param <T>          Type of object to deserialize
     * @return Deserializer that uses user supplied deserialization function for deserializing payload into typed events.
     */
    public static <T> Serializer<T> customDeserializer(SerializerConfig config, @Nullable Schema<T> schema,
                                                       CustomDeserializer<T> deserializer) {
        return CustomSerializerFactory.deserializer(config, schema, deserializer);
    }
    // endregion

    // region multiformat
    /**
     * A multi format serializer that takes objects with schemas for the three supported formats - avro, protobuf and json.
     * It then serializes the object using the format specific serializer. The events are supplied using an encapsulating 
     * object called WithSchema which has both the event and the schema. 
     * It only serializes the events while ensuring that the corresponding schema was registered with the service. 
     * If {@link SerializerConfig#registerSchema} is set to true, it registers the schema before using it. 
     * This serializer contacts schema registry once for every new schema that it encounters, and it fetches the 
     * encoding id for the schema and codec pair. 
     *
     * @param config Serializer config
     * @return A multi format serializer which serializes events from all three of Avro, Protobuf and json formats. 
     */
    public static Serializer<WithSchema<Object>> serializerWithSchema(SerializerConfig config) {
        return MultiFormatSerializerFactory.serializer(config);
    }

    /**
     * A deserializer that can deserialize data where each event could be written with either of avro, protobuf or json 
     * serialization formats. It deserializes them into format specific generic objects. 
     * An event serialized with avro is deserialized into {@link GenericRecord} or {@link Object} with schema as {@link org.apache.avro.Schema}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage} with schema as {@link FileDescriptorSet}.
     * An event serialized with json is deserialized into a {@link JsonNode} with schema as {@link JsonSchema}.
     * The object and schema are wrapped in {@link WithSchema} object. 
     *
     * @param config serializer config
     * @return a deserializer that can deserialize events serialized as protobuf, json or avro into java objects.
     */
    public static Serializer<WithSchema<Object>> deserializerWithSchema(SerializerConfig config) {
        return MultiFormatSerializerFactory.deserializerWithSchema(config);
    }

    /**
     * A deserializer that can read data where each event could be written with either of avro, protobuf or json 
     * serialization formats.
     * An event serialized with avro is deserialized into {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link WithSchema} object of {@link JsonNode} and {@link JsonSchema}.
     *
     * @param config serializer config
     * @return a deserializer that can deserialize events serialized as protobuf, json or avro into java objects.
     */
    public static Serializer<Object> genericDeserializer(SerializerConfig config) {
        return deserializeAsT(config, NO_TRANSFORM);
    }

    /**
     * This is a convenience serializer shortcut that calls {@link #deserializeAsT} with a transform to 
     * convert the object to JSON string.
     *
     * @param config serializer config
     * @return a deserializer that can deserialize protobuf, json or avro events into java objects.
     */
    public static Serializer<String> deserializeAsJsonString(SerializerConfig config) {
        return deserializeAsT(config, JSON_TRANSFORM);
    }

    /**
     * A deserializer that can read data where each event could be written with different serialization formats. 
     * Formats supported are protobuf, avro and json. 
     * An event serialized with avro is deserialized into {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link WithSchema} object of {@link JsonNode} and {@link JsonSchema}.
     *
     * This also takes a transform function which is applied on the deserialized object and should transform the object 
     * into the type T.  
     *
     * @param config serializer config
     * @param transform a transform function that transforms the deserialized object based on the serialization format 
     *                  into an object of type T. 
     * @param <T> Type of object to get back from deserializer. 
     * @return a deserializer that can deserialize protobuf, json or avro events into java objects.
     */
    public static <T> Serializer<T> deserializeAsT(SerializerConfig config,
                                                   BiFunction<SerializationFormat, Object, T> transform) {
        return MultiFormatSerializerFactory.deserializeAsT(config, transform);
    }
    // endregion
}
