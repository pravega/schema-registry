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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;

import static io.pravega.schemaregistry.serializers.SerializerFactoryHelper.*;

/**
 * Internal Factory class for Custom serializers and deserializers. 
 */
@Slf4j
class CustomSerializerFactory {
    static <T> Serializer<T> serializer(SerializerConfig config, SchemaContainer<T> schema, PravegaSerializer<T> serializer) {
        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return new AbstractPravegaSerializer<T>(groupId, schemaRegistryClient,
                schema, config.getCodec(), config.isRegisterSchema()) {
            @Override
            protected void serialize(T var, SchemaInfo schema, OutputStream outputStream) {
                serializer.serialize(var, schema, outputStream);
            }
        };
    }

    static <T> Serializer<T> deserializer(SerializerConfig config, @Nullable SchemaContainer<T> schema,
                                          PravegaDeserializer<T> deserializer) {

        String groupId = config.getGroupId();
        SchemaRegistryClient schemaRegistryClient = config.getRegistryConfigOrClient().isLeft() ?
                SchemaRegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();
        autoCreateGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);

        EncodingCache encodingCache = new EncodingCache(groupId, schemaRegistryClient);

        return new AbstractPravegaDeserializer<T>(groupId, schemaRegistryClient, schema, false,
                config.getDecoder(), encodingCache) {
            @Override
            protected T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
                return deserializer.deserialize(inputStream, writerSchema, readerSchema);
            }
        };
    }
}
