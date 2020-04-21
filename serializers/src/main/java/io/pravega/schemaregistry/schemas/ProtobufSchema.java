/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.schemas;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Container class for protobuf schema.
 * Protobuf schemas are represented using {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet}. 
 * 
 * @param <T> Type of element. 
 */
@Data
public class ProtobufSchema<T extends Message> implements SchemaContainer<T> {
    @Getter
    private final Parser<T> parser;
    @Getter
    private final DescriptorProtos.FileDescriptorSet descriptorProto;
    
    private final SchemaInfo schemaInfo;

    private ProtobufSchema(String name, Parser<T> parser, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        this.parser = parser;
        this.descriptorProto = fileDescriptorSet;
        this.schemaInfo = new SchemaInfo(name, SchemaType.Protobuf, 
                getSchemaBytes(), ImmutableMap.of());
    }
    
    private byte[] getSchemaBytes() {
        return descriptorProto.toByteArray();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    /**
     * Method to generate protobuf schema from the supplied protobuf generated class and {@link DescriptorProtos.FileDescriptorSet}.
     * 
     * @param tClass Class for code generated protobuf message.  
     * @param fileDescriptorSet file descriptor set representing a protobuf schema. 
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type T that captures protobuf schema and parser. 
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> tClass, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
        Parser<T> tParser = (Parser<T>) defaultInstance.getParserForType();
        return new ProtobufSchema<>(defaultInstance.getDescriptorForType().getFullName(), tParser, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema of generic type {@link DynamicMessage} using the {@link DescriptorProtos.FileDescriptorSet}.
     * It is for representing protobuf schemas to be used for generic deserialization of protobuf serialized payload into
     * {@link DynamicMessage}.
     * Note: this does not have a protobuf parser and can only be used during deserialization.
     *
     * @param name              Name of protobuf message
     * @param fileDescriptorSet file descriptor set representing a protobuf schema.
     * @return {@link ProtobufSchema} with generic type {@link DynamicMessage} that captures protobuf schema.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static ProtobufSchema<DynamicMessage> of(String name, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        return new ProtobufSchema<>(name, null, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema from the supplied protobuf generated class and {@link DescriptorProtos.FileDescriptorSet}.
     * It is same as {@link #of(Class, DescriptorProtos.FileDescriptorSet)} except that it returns a Protobuf schema
     * typed {@link GeneratedMessageV3}.
     * It is useful in multiplexed deserializer to pass all objects to deserialize into as base {@link GeneratedMessageV3} objects. 
     *
     * @param tDerivedClass Class for code generated protobuf message.  
     * @param fileDescriptorSet file descriptor set representing a protobuf schema. 
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type {@link GeneratedMessageV3} that captures protobuf schema and parser of type T. 
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> ofBaseType(Class<? extends T> tDerivedClass, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance = (T) tDerivedClass.getMethod("getDefaultInstance").invoke(null);
        Parser<T> tParser = (Parser<T>) defaultInstance.getParserForType();

        return new ProtobufSchema<>(defaultInstance.getDescriptorForType().getFullName(), tParser, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema of generic type {@link DynamicMessage} from schemaInfo {@link SchemaInfo}.
     *
     * @param schemaInfo              Schema Info
     * @return {@link ProtobufSchema} with generic type {@link DynamicMessage} that captures protobuf schema.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static ProtobufSchema<DynamicMessage> from(SchemaInfo schemaInfo) {
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaInfo.getSchemaData());

        return new ProtobufSchema<>(schemaInfo.getName(), null, fileDescriptorSet);
    }
}

