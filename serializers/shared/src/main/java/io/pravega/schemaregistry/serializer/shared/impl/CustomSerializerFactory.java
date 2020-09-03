/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.impl;

import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.shared.schemas.Schema;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;

import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForDeserializer;
import static io.pravega.schemaregistry.serializer.shared.impl.SerializerFactoryHelper.initForSerializer;

/**
 * Internal Factory class for Custom serializers and deserializers. 
 */
@Slf4j
public class CustomSerializerFactory {
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
    public static <T> Serializer<T> serializer(@NonNull SerializerConfig config, @NonNull Schema<T> schema, @NonNull CustomSerializer<T> serializer) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForSerializer(config);
        return new AbstractSerializer<T>(groupId, schemaRegistryClient,
                schema, config.getEncoder(), config.isRegisterSchema(), config.isWriteEncodingHeader()) {
            @Override
            protected void serialize(T var, SchemaInfo schema, OutputStream outputStream) {
                serializer.serialize(var, schema, outputStream);
            }
        };
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
    public static <T> Serializer<T> deserializer(@NonNull SerializerConfig config, @Nullable Schema<T> schema,
                                                 @NonNull CustomDeserializer<T> deserializer) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = initForDeserializer(config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AbstractDeserializer<T>(groupId, schemaRegistryClient, schema, false,
                config.getDecoders(), encodingCache, config.isWriteEncodingHeader()) {
            @Override
            public final T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                return deserializer.deserialize(inputStream, writerSchema, readerSchema);
            }
        };
    }
}
