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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.function.BiFunction;

/**
 * Container class for a deserialized object with its corresponding schema.
 * 
 * @param <T> Type of object.
 */
public class WithSchema<T> {
    public static final BiFunction<SerializationFormat, Object, String> JSON_TRANSFORM = WithSchema::toJsonString;
    
    public static final BiFunction<SerializationFormat, Object, Object> NO_TRANSFORM = (x, y) -> y;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames()
                                                                .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().build());

    @Getter(AccessLevel.PACKAGE)
    private final Schema schema;
    @Getter
    private final Object object;
    private final BiFunction<SerializationFormat, Object, T> transform;
    
    WithSchema(SchemaInfo schemaInfo, Object obj, BiFunction<SerializationFormat, Object, T> transform) {
        this.object = obj;
        this.transform = transform;
        if (schemaInfo != null) {
            this.schema = convertToSchema(schemaInfo);
        } else {
            this.schema = null;
        }
    }

    private Schema convertToSchema(SchemaInfo schemaInfo) {
        Schema schema;
        switch (schemaInfo.getSerializationFormat()) {
            case Protobuf:
                schema = ProtobufSchema.from(schemaInfo);
                break;
            case Custom:
                schema = new Schema<Object>() {
                    @Override
                    public SchemaInfo getSchemaInfo() {
                        return schemaInfo;
                    }

                    @Override
                    public Class<Object> getTClass() {
                        return Object.class;
                    }
                };
                break;
            default:
                throw new IllegalArgumentException("Serialization format not supported");
        }
        return schema;
    }
    
    /**
     * Check whether the schema is of type Protobuf.
     * 
     * @return True if the schema is for protobuf, false otherwise.
     */
    public boolean hasProtobufSchema() {
        return schema instanceof ProtobufSchema;    
    }

    /**
     * Protobuf Schema for the underlying deserialized object. This is available if {@link WithSchema#hasProtobufSchema()} returns true.
     * This means underlying object was serialized as protobuf. 
     * 
     * @return Protobuf {@link DescriptorProtos.FileDescriptorSet} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public DescriptorProtos.FileDescriptorSet getProtobufSchema() {
        return ((ProtobufSchema<DynamicMessage>) schema).getFileDescriptorSet();
    }
    
    /**
     * Applies the transform on the deserialized object. 
     * 
     * @return Transformed object of type T. 
     */
    public T getTransformed() {
        if (schema == null) {
            throw new IllegalArgumentException("Need schema to be able to transform.");
        }
        return transform.apply(schema.getSchemaInfo().getSerializationFormat(), object);
    }

    /**
     * Applies JsonString transformation to convert the deserialized object into a json string. 
     * 
     * @return Json String for the object. 
     */
    public String getJsonString() {
        if (schema == null) {
            throw new IllegalArgumentException();
        }
        return JSON_TRANSFORM.apply(schema.getSchemaInfo().getSerializationFormat(), object);
    }

    private static String toJsonString(SerializationFormat format, Object deserialize) {
        String jsonString;
        try {
            switch (format) {
                case Protobuf:
                    jsonString = PRINTER.print((DynamicMessage) deserialize);
                    break;
                case Json:
                    if (deserialize instanceof WithSchema) {
                        jsonString = OBJECT_MAPPER.writeValueAsString(((WithSchema) deserialize).object);
                    } else {
                        jsonString = OBJECT_MAPPER.writeValueAsString(deserialize);
                    }
                    break;
                default:
                    jsonString = OBJECT_MAPPER.writeValueAsString(deserialize);
            }
        } catch (InvalidProtocolBufferException | JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid deserialized object. Failed to convert to json string.", e);
        }
        return jsonString;
    }
    
    /**
     * Create WithSchema object for protobuf. 
     *
     * @param object Object.
     * @param protobufSchema Protobuf Schema for object.
     * @param <T> Type of object. 
     * @return A WithSchema object which has Protobuf Schema and the corresponding object.  
     */
    public static <T extends GeneratedMessageV3> WithSchema<T> proto(T object, ProtobufSchema<T> protobufSchema) {
        Preconditions.checkNotNull(object, "object cannot be null");
        Preconditions.checkNotNull(protobufSchema, "schema cannot be null");
        return new WithSchema<>(protobufSchema.getSchemaInfo(), object, (x, y) -> object);
    }
}
