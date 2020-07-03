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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.Data;
import lombok.Getter;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

/**
 * Container class for protobuf schema.
 * Protobuf schemas are represented using {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet}. 
 *
 * @param <T> Type of element. 
 */
@Data
public class ProtobufSchema<T extends Message> implements Schema<T> {
    @Getter
    private final Parser<T> parser;
    @Getter
    private final DescriptorProtos.FileDescriptorSet descriptorProto;

    private final SchemaInfo schemaInfo;

    private ProtobufSchema(String name, Parser<T> parser, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        this.parser = parser;
        this.descriptorProto = fileDescriptorSet;
        this.schemaInfo = new SchemaInfo(name, SerializationFormat.Protobuf, getSchemaBytes(), ImmutableMap.of());
    }

    private ProtobufSchema(DescriptorProtos.FileDescriptorSet fileDescriptorSet, SchemaInfo schemaInfo) {
        this.parser = null;
        this.descriptorProto = fileDescriptorSet;
        this.schemaInfo = schemaInfo;
    }
    
    private ByteBuffer getSchemaBytes() {
        return ByteBuffer.wrap(descriptorProto.toByteArray());
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
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> tClass, 
                                                                      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance;
        try {
            defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
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
    public static ProtobufSchema<DynamicMessage> of(String name, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        return new ProtobufSchema<>(name, null, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema from the supplied protobuf generated class and {@link DescriptorProtos.FileDescriptorSet}.
     * It is same as {@link #of(Class, DescriptorProtos.FileDescriptorSet)} except that it returns a Protobuf schema
     * typed {@link GeneratedMessageV3}.
     * It is useful in multiplexed deserializer to pass all objects to deserialize into as base {@link GeneratedMessageV3} objects. 
     *
     * @param tClass Class for code generated protobuf message.  
     * @param fileDescriptorSet file descriptor set representing a protobuf schema. 
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type {@link GeneratedMessageV3} that captures protobuf schema and parser of type T. 
     */
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<GeneratedMessageV3> ofGeneratedMessageV3(
            Class<T> tClass, DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
        T defaultInstance;
        try {
            defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
        Parser<GeneratedMessageV3> tParser = (Parser<GeneratedMessageV3>) defaultInstance.getParserForType();

        return new ProtobufSchema<>(defaultInstance.getDescriptorForType().getFullName(), tParser, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema of generic type {@link DynamicMessage} from schemaInfo {@link SchemaInfo}.
     *
     * @param schemaInfo              Schema Info
     * @return {@link ProtobufSchema} with generic type {@link DynamicMessage} that captures protobuf schema.
     */
    public static ProtobufSchema<DynamicMessage> from(SchemaInfo schemaInfo) {
        try {
            DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaInfo.getSchemaData());

            return new ProtobufSchema<>(fileDescriptorSet, schemaInfo);
        } catch (InvalidProtocolBufferException ex) {
            throw new IllegalArgumentException(ex);
        }
    }
}

