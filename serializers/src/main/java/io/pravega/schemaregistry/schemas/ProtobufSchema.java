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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import static com.google.protobuf.DescriptorProtos.*;

/**
 * Container class for protobuf schema.
 * Protobuf schemas are represented using {@link FileDescriptorSet}. 
 *
 * @param <T> Type of element. 
 */
@Data
public class ProtobufSchema<T extends Message> implements Schema<T> {
    @Getter
    private final Parser<T> parser;
    @Getter
    private final FileDescriptorSet descriptorProto;

    private final SchemaInfo schemaInfo;

    private ProtobufSchema(String name, Parser<T> parser, FileDescriptorSet fileDescriptorSet) {
        this.parser = parser;
        this.descriptorProto = fileDescriptorSet;
        this.schemaInfo = new SchemaInfo(name, SerializationFormat.Protobuf, getSchemaBytes(), ImmutableMap.of());
    }

    private ProtobufSchema(FileDescriptorSet fileDescriptorSet, SchemaInfo schemaInfo) {
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
     * Method to generate protobuf schema from the supplied protobuf generated class.
     * If the description of protobuf object is contained in a single .proto file, then this method creates the 
     * {@link FileDescriptorSet} from the generated class. 
     *
     * @param tClass Class for code generated protobuf message.  
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type T that captures protobuf schema and parser. 
     */
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> tClass) {
        Extractor<T> extractor = new Extractor<>(tClass).invoke();
        
        return new ProtobufSchema<T>(extractor.getFullName(), extractor.getParser(), 
                extractor.getFileDescriptorSet());
    }
    
    /**
     * Method to generate protobuf schema from the supplied protobuf generated class and {@link FileDescriptorSet}.
     *
     * @param tClass Class for code generated protobuf message.  
     * @param fileDescriptorSet file descriptor set representing a protobuf schema. 
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type T that captures protobuf schema and parser. 
     */
    public static <T extends GeneratedMessageV3> ProtobufSchema<T> of(Class<T> tClass, FileDescriptorSet fileDescriptorSet) {
        Extractor<T> extractor = new Extractor<>(tClass).invoke();
        return new ProtobufSchema<T>(extractor.getFullName(), extractor.getParser(), fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema of generic type {@link DynamicMessage} using the {@link FileDescriptorSet}.
     * It is for representing protobuf schemas to be used for generic deserialization of protobuf serialized payload into
     * {@link DynamicMessage}.
     * Note: this does not have a protobuf parser and can only be used during deserialization.
     *
     * @param name              Name of protobuf message
     * @param fileDescriptorSet file descriptor set representing a protobuf schema.
     * @return {@link ProtobufSchema} with generic type {@link DynamicMessage} that captures protobuf schema.
     */
    public static ProtobufSchema<DynamicMessage> of(String name, FileDescriptorSet fileDescriptorSet) {
        return new ProtobufSchema<>(name, null, fileDescriptorSet);
    }

    /**
     * Method to generate protobuf schema from the supplied protobuf generated class and {@link FileDescriptorSet}.
     * It is same as {@link #of(Class, FileDescriptorSet)} except that it returns a Protobuf schema
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
            Class<T> tClass, FileDescriptorSet fileDescriptorSet) {
        Extractor<T> extractor = new Extractor<>(tClass).invoke();

        return new ProtobufSchema<>(extractor.getFullName(), (Parser<GeneratedMessageV3>) extractor.getParser(), fileDescriptorSet);
    }
    
    /**
     * Method to generate protobuf schema from the supplied protobuf generated class. It creates the {@link FileDescriptorSet}
     * from the generated class.
     * This method is same as {@link #of(Class)} except that it returns a Protobuf schema
     * typed {@link GeneratedMessageV3}.
     * It is useful in multiplexed deserializer to pass all objects to deserialize into as base {@link GeneratedMessageV3} objects. 
     *
     * @param tClass Class for code generated protobuf message.  
     * @param <T> Type of protobuf message
     * @return {@link ProtobufSchema} with generic type {@link GeneratedMessageV3} that captures protobuf schema and parser of type T. 
     */
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> ProtobufSchema<GeneratedMessageV3> ofGeneratedMessageV3(Class<T> tClass) {
        Extractor<T> extractor = new Extractor<>(tClass).invoke();

        return new ProtobufSchema<>(extractor.getFullName(),
                (Parser<GeneratedMessageV3>) extractor.getParser(), extractor.getFileDescriptorSet());
    }

    /**
     * Method to generate protobuf schema of generic type {@link DynamicMessage} from schemaInfo {@link SchemaInfo}.
     *
     * @param schemaInfo              Schema Info
     * @return {@link ProtobufSchema} with generic type {@link DynamicMessage} that captures protobuf schema.
     */
    public static ProtobufSchema<DynamicMessage> from(SchemaInfo schemaInfo) {
        try {
            FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(schemaInfo.getSchemaData());

            return new ProtobufSchema<>(fileDescriptorSet, schemaInfo);
        } catch (InvalidProtocolBufferException ex) {
            throw new IllegalArgumentException("Unable to get protobuf schema from schemainfo", ex);
        }
    }

    private static class Extractor<T extends GeneratedMessageV3> {
        @Getter(AccessLevel.PRIVATE)
        private Class<T> tClass;
        @Getter(AccessLevel.PRIVATE)
        private T defaultInstance;
        @Getter(AccessLevel.PRIVATE)
        private Parser<T> parser;

        Extractor(Class<T> tClass) {
            this.tClass = tClass;
        }

        String getFullName() {
            return defaultInstance.getDescriptorForType().getFullName();
        }

        FileDescriptorSet getFileDescriptorSet() {
            // TODO: verify that the file proto has descriptors for all message types
            return FileDescriptorSet
                    .newBuilder().addFile(defaultInstance.getDescriptorForType().getFile().toProto()).build();
        }
        
        @SuppressWarnings("unchecked")
        Extractor<T> invoke() {
            try {
                defaultInstance = (T) tClass.getMethod("getDefaultInstance").invoke(null);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new IllegalArgumentException(e);
            }
            parser = (Parser<T>) defaultInstance.getParserForType();
            return this;
        }
    }
}

