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
import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.initForSerializer;
import static io.pravega.schemaregistry.serializers.WithSchema.NO_TRANSFORM;

/**
 * Internal Factory class for multi format serializers and deserializers. 
 * These serializers can be used to work with streams when either you dont know the format beforehand or the stream allows
 * for multiple formats. 
 */
@Slf4j
class MultiFormatSerializerFactory {
    // region multi format 
    static Serializer<WithSchema<Object>> serializer(SerializerConfig config) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        return serializerInternal(config, Collections.emptyMap());
    }

    static Serializer<WithSchema<Object>> deserializerWithSchema(SerializerConfig config) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        return deserializerInternal(config, Collections.emptyMap(), NO_TRANSFORM);
    }
    
    /**
     * A deserializer that can read data where each event could be written with different serialization formats. 
     * Formats supported are protobuf, avro and json. 
     * An event serialized with avro is deserialized into {@link GenericRecord}.
     * An event serialized with protobuf is deserialized into {@link DynamicMessage}.
     * An event serialized with json is deserialized into {@link java.util.LinkedHashMap}.
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
    static <T> Serializer<T> deserializeAsT(SerializerConfig config,
                                            BiFunction<SerializationFormat, Object, T> transform) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(transform);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        return deserializeAsTInternal(config, Collections.emptyMap(), transform);
    }
    // endregion

    private static Serializer<WithSchema<Object>> serializerInternal(SerializerConfig config,
                                                                     Map<SerializationFormat, CustomSerializer<Object>> customSerializers) {
        Preconditions.checkNotNull(config);
        Preconditions.checkArgument(config.isWriteEncodingHeader(), "Events should be tagged with encoding ids.");
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        String groupId = config.getGroupId();

        // if serializer is not already present, create a new serializer. 
        Function<SchemaInfo, AbstractSerializer<Object>> serializerFunction =
                x -> getPravegaSerializer(config, customSerializers, schemaRegistryClient, groupId, x);
        return new MultipleFormatSerializer(serializerFunction);
    }

    private static <T> Serializer<T> deserializeAsTInternal(SerializerConfig config,
                                                    Map<SerializationFormat, CustomDeserializer<Object>> deserializers,
                                                    BiFunction<SerializationFormat, Object, T> transform) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        AbstractDeserializer json = new JsonGenericDeserializer(config.getGroupId(), schemaRegistryClient,
                config.getDecoder(), encodingCache, config.isWriteEncodingHeader());
        AbstractDeserializer protobuf = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache, config.isWriteEncodingHeader());
        AbstractDeserializer avro = new AvroGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);

        Map<SerializationFormat, AbstractDeserializer> map = new HashMap<>();
        map.put(SerializationFormat.Json, json);
        map.put(SerializationFormat.Avro, avro);
        map.put(SerializationFormat.Protobuf, protobuf);

        deserializers.forEach((key, value) -> {
            map.put(key, new AbstractDeserializer<Object>(groupId, schemaRegistryClient, null, false, 
                    config.getDecoder(), encodingCache, config.isWriteEncodingHeader()) {
                @Override
                protected Object deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                    return value.deserialize(inputStream, writerSchema, readerSchema);
                }
            });
        });

        return new MultipleFormatDeserializer<>(groupId, schemaRegistryClient, map, config.getDecoder(),
                encodingCache, transform);
    }

    private static <T> Serializer<WithSchema<T>> deserializerInternal(SerializerConfig config, Map<SerializationFormat,
            CustomDeserializer<Object>> deserializers, BiFunction<SerializationFormat, Object, T> transform) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);
        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        AbstractDeserializer json = new JsonGenericDeserializer(config.getGroupId(), schemaRegistryClient,
                config.getDecoder(), encodingCache, config.isWriteEncodingHeader());
        AbstractDeserializer protobuf = new ProtobufGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache, config.isWriteEncodingHeader());
        AbstractDeserializer avro = new AvroGenericDeserlizer(groupId, schemaRegistryClient, null, config.getDecoder(),
                encodingCache);

        Map<SerializationFormat, AbstractDeserializer> map = new HashMap<>();
        map.put(SerializationFormat.Json, json);
        map.put(SerializationFormat.Avro, avro);
        map.put(SerializationFormat.Protobuf, protobuf);

        deserializers.forEach((key, value) -> {
            map.put(key, new AbstractDeserializer<Object>(groupId, schemaRegistryClient, null, false, 
                    config.getDecoder(), encodingCache, config.isWriteEncodingHeader()) {
                @Override
                protected Object deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                    return value.deserialize(inputStream, writerSchema, readerSchema);
                }
            });
        });

        return new MultiFormatWithSchemaDeserializer<>(groupId, schemaRegistryClient, map, config.getDecoder(),
                encodingCache, transform);
    }

    @SuppressWarnings("unchecked")
    private static AbstractSerializer<Object> getPravegaSerializer(
            SerializerConfig config, Map<SerializationFormat, CustomSerializer<Object>> customSerializers,
            SchemaRegistryClient schemaRegistryClient, String groupId, SchemaInfo schemaInfo) {
        switch (schemaInfo.getSerializationFormat()) {
            case Avro:
                return new AvroSerializer<>(groupId, schemaRegistryClient,
                        AvroSchema.from(schemaInfo), config.getCodec(), config.isRegisterSchema());
            case Protobuf:
                ProtobufSerializer<?> m = new ProtobufSerializer<>(groupId, schemaRegistryClient,
                        ProtobufSchema.from(schemaInfo), config.getCodec(), config.isRegisterSchema(), config.isWriteEncodingHeader());
                return (AbstractSerializer<Object>) m;
            case Json:
                return new JsonSerializer<>(groupId, schemaRegistryClient, JSONSchema.from(schemaInfo),
                        config.getCodec(), config.isRegisterSchema(), config.isWriteEncodingHeader());
            case Custom:
                return getCustomSerializer(config, customSerializers, schemaRegistryClient, groupId, schemaInfo);
            default:
                throw new IllegalArgumentException("Serializer not provided");
        }
    }

    private static AbstractSerializer<Object> getCustomSerializer(
            SerializerConfig config, Map<SerializationFormat, CustomSerializer<Object>> customSerializers, 
            SchemaRegistryClient schemaRegistryClient, String groupId, SchemaInfo schemaInfo) {
        if (customSerializers.containsKey(schemaInfo.getSerializationFormat())) {
            CustomSerializer<Object> serializer = customSerializers.get(schemaInfo.getSerializationFormat());
            return new AbstractSerializer<Object>(groupId, schemaRegistryClient,
                    () -> schemaInfo, config.getCodec(), config.isRegisterSchema(), config.isWriteEncodingHeader()) {
                @Override
                protected void serialize(Object var, SchemaInfo schema, OutputStream outputStream) {
                    serializer.serialize(var, schema, outputStream);
                }
            };
        } else {
            throw new IllegalArgumentException("Serializer for the format not supplied");
        }
    }
}
