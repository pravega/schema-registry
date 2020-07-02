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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.function.BiFunction;

/**
 * Container class for object with its corresponding schema. 
 * @param <T> Type of object.
 */
public class WithSchema<T> {
    public static final BiFunction<SerializationFormat, Object, String> JSON_TRANSFORM = WithSchema::toJsonString;
    public static final BiFunction<SerializationFormat, Object, Object> NO_TRANSFORM = (x, y) -> y;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames()
                                                                .usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().build());

    @Getter(AccessLevel.PACKAGE)
    private final SchemaContainer schemaContainer;
    @Getter
    private final Object object;
    private final BiFunction<SerializationFormat, Object, T> transform;
    
    WithSchema(SchemaInfo schemaInfo, Object obj, BiFunction<SerializationFormat, Object, T> transform) {
        this.object = obj;
        this.transform = transform;
        if (schemaInfo != null) {
            switch (schemaInfo.getSerializationFormat()) {
                case Avro:
                    schemaContainer = AvroSchema.from(schemaInfo);
                    break;
                case Protobuf:
                    schemaContainer = ProtobufSchema.from(schemaInfo);
                    break;
                case Json:
                    schemaContainer = JSONSchema.from(schemaInfo);
                    break;
                case Custom:
                    schemaContainer = () -> schemaInfo;
                    break;
                default:
                    throw new IllegalArgumentException("Serialization format not supported");
            }
        } else {
            schemaContainer = null;
        }
    }

    /**
     * Check whether the schema is of type Avro.
     * 
     * @return True if the schema is for avro, false otherwise.
     */
    public boolean hasAvroSchema() {
        return schemaContainer instanceof AvroSchema;    
    }

    /**
     * Avro Schema for the underlying deserialized object. This is available if {@link this#hasAvroSchema()} returns true.
     * This means underlying object was serialized as avro. 
     *
     * @return Protobuf {@link Schema} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public Schema getAvroSchema() {
        return ((AvroSchema<Object>) schemaContainer).getSchema();
    }

    /**
     * Check whether the schema is of type Protobuf.
     * 
     * @return True if the schema is for protobuf, false otherwise.
     */
    public boolean hasProtobufSchema() {
        return schemaContainer instanceof ProtobufSchema;    
    }

    /**
     * Protobuf Schema for the underlying deserialized object. This is available if {@link this#hasProtobufSchema()} returns true.
     * This means underlying object was serialized as protobuf. 
     * 
     * @return Protobuf {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public DescriptorProtos.FileDescriptorSet getProtobufSchema() {
        return ((ProtobufSchema<DynamicMessage>) schemaContainer).getDescriptorProto();
    }

    /**
     * Check whether the schema is of type Json.
     * 
     * @return True if the schema is for json, false otherwise
     */
    public boolean hasJsonSchema() {
        return schemaContainer instanceof JSONSchema;    
    }

    /**
     * Json Schema for the underlying deserialized object. This is available if {@link this#hasJsonSchema()} returns true.
     * This means underlying object was serialized as Json. 
     *
     * @return Protobuf {@link JsonSchema} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public JsonSchema getJsonSchema() {
        return ((JSONSchema<Object>) schemaContainer).getSchema();
    }

    /**
     * Applies the transform on the deserialized object. 
     * 
     * @return Transformed object of type T. 
     */
    public T getTransformed() {
        if (schemaContainer == null) {
            throw new IllegalArgumentException();
        }
        return transform.apply(schemaContainer.getSchemaInfo().getSerializationFormat(), object);
    }

    /**
     * Applies JsonString transformation to convert the deserialized object into a json string. 
     * 
     * @return Json String for the object. 
     */
    public String getJsonString() {
        if (schemaContainer == null) {
            throw new IllegalArgumentException();
        }
        return JSON_TRANSFORM.apply(schemaContainer.getSchemaInfo().getSerializationFormat(), object);
    }

    @SneakyThrows
    private static String toJsonString(SerializationFormat format, Object deserialize) {
        String jsonString;
        switch (format) {
            case Avro:
                if (deserialize instanceof IndexedRecord) {
                    jsonString = deserialize.toString();
                } else {
                    jsonString = OBJECT_MAPPER.writeValueAsString(deserialize);
                }
                break;
            case Protobuf:
                jsonString = PRINTER.print((DynamicMessage) deserialize);
                break;
            case Json:
                jsonString = OBJECT_MAPPER.writeValueAsString(((WithSchema) deserialize).object);
                break;
            default:
                jsonString = OBJECT_MAPPER.writeValueAsString(deserialize);
        }
        return jsonString;
    }
}
