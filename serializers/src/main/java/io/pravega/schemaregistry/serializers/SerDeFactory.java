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
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.RegistryClientFactory;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;

public class SerDeFactory {
    // region avro
    public static <T> Serializer<T> avroSerializer(SerializerConfig config, AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);
        return new AvroSerializer<>(groupId, registryClient, schemaData, config.getCompressor(),
                config.isAutoRegisterSchema(), encodingCache);
    }

    public static <T extends IndexedRecord> Serializer<T> avroDeserializer(SerializerConfig config, 
                                                                           AvroSchema<T> schemaData) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(schemaData);
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();
        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new AvroDeserlizer<>(groupId, registryClient, schemaData, config.getUncompress(), encodingCache);
    }

    public static Serializer<GenericRecord> genericAvroDeserializer(SerializerConfig config, 
                                                                    @Nullable AvroSchema<GenericRecord> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new AvroGenericDeserlizer(groupId, registryClient, schemaData, config.getUncompress(), encodingCache);
    }

    public static <T> Serializer<T> multiplexedAvroSerializer(SerializerConfig config,
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
                        x -> new AvroSerializer<>(groupId, registryClient, x.getValue(), config.getCompressor(),
                                config.isAutoRegisterSchema(), encodingCache)));
        return new MultiplexedSerializer<>(serializerMap);
    }

    public static <T extends SpecificRecordBase> Serializer<T> multiplexedAvroDeserializer(
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
                        x -> new AvroDeserlizer<>(groupId, registryClient, x.getValue(), config.getUncompress(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, registryClient,
                deserializerMap, false, config.getUncompress(), encodingCache);
    }

    // endregion
    
    // region protobuf
    public static <T extends GeneratedMessageV3> Serializer<T> protobufSerializer(SerializerConfig config, 
                                                                                  ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new ProtobufSerializer<>(groupId, registryClient, schemaData, config.getCompressor(),
                config.isAutoRegisterSchema(), encodingCache);
    }

    public static <T extends GeneratedMessageV3> Serializer<T> protobufDeserializer(SerializerConfig config, 
                                                                                    ProtobufSchema<T> schemaData) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        // schema can be null in which case deserialization will happen into dynamic message
        return new ProtobufDeserlizer<>(groupId, registryClient, schemaData, config.getUncompress(), encodingCache);
    }

    public static Serializer<DynamicMessage> genericProtobufDeserializer(SerializerConfig config, ProtobufSchema<DynamicMessage> schema) {
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        String groupId = config.getGroupId();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        return new ProtobufGenericDeserlizer(groupId, registryClient, schema, config.getUncompress(), encodingCache);
    }

    public static <T extends GeneratedMessageV3> Serializer<T> multiplexedProtobufSerializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        x -> new ProtobufSerializer<>(groupId, registryClient, x.getValue(), config.getCompressor(),
                                config.isAutoRegisterSchema(), encodingCache)));
        return new MultiplexedSerializer<>(serializerMap);
    }

    public static <T extends GeneratedMessageV3> Serializer<T> multiplexedProtobufDeserializer(
            SerializerConfig config, Map<Class<? extends T>, ProtobufSchema<T>> schemas) {
        String groupId = config.getGroupId();
        SchemaRegistryClient registryClient = config.getRegistryConfigOrClient().isLeft() ?
                RegistryClientFactory.createRegistryClient(config.getRegistryConfigOrClient().getLeft()) :
                config.getRegistryConfigOrClient().getRight();

        EncodingCache encodingCache = new EncodingCache(groupId, registryClient);

        Map<String, AbstractPravegaDeserializer<T>> deserializerMap = schemas
                .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getSimpleName(),
                        x -> new ProtobufDeserlizer<>(groupId, registryClient, x.getValue(), config.getUncompress(), encodingCache)));
        return new MultiplexedDeserializer<>(groupId, registryClient,
                deserializerMap, false, config.getUncompress(), encodingCache);
    }
    //endregion
}
