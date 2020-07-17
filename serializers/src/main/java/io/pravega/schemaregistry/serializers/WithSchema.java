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
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.schemas.JSONSchema;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.schemas.Schema;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.avro.generic.IndexedRecord;

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
            case Avro:
                schema = AvroSchema.from(schemaInfo);
                break;
            case Protobuf:
                schema = ProtobufSchema.from(schemaInfo);
                break;
            case Json:
                schema = JSONSchema.from(schemaInfo);
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
     * Check whether the schema is of type Avro.
     * 
     * @return True if the schema is for avro, false otherwise.
     */
    public boolean hasAvroSchema() {
        return schema instanceof AvroSchema;    
    }

    /**
     * Avro Schema for the underlying deserialized object. This is available if {@link WithSchema#hasAvroSchema()} returns true.
     * This means underlying object was serialized as avro. 
     *
     * @return Protobuf {@link org.apache.avro.Schema} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public org.apache.avro.Schema getAvroSchema() {
        return ((AvroSchema<Object>) schema).getSchema();
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
     * @return Protobuf {@link com.google.protobuf.DescriptorProtos.FileDescriptorSet} representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public DescriptorProtos.FileDescriptorSet getProtobufSchema() {
        return ((ProtobufSchema<DynamicMessage>) schema).getFileDescriptorSet();
    }

    /**
     * Check whether the schema is of type Json.
     * 
     * @return True if the schema is for json, false otherwise
     */
    public boolean hasJsonSchema() {
        return schema instanceof JSONSchema;    
    }

    /**
     * Json Schema for the underlying deserialized object. This is available if {@link WithSchema#hasJsonSchema()} returns true.
     * This means underlying object was serialized as Json. 
     *
     * @return Json schema String representing the schema for the object. 
     */
    @SuppressWarnings("unchecked")
    public String getJsonSchema() {
        return ((JSONSchema<Object>) schema).getSchemaString();
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
     * Create WithSchema object for avro. 
     * 
     * @param object Object.
     * @param avroSchema Avro Schema for object.
     * @param <T> Type of object. 
     * @return A WithSchema object which has Avro Schema and the corresponding object.  
     */
    public static <T> WithSchema<T> avro(T object, AvroSchema<T> avroSchema) {
        Preconditions.checkNotNull(object, "object cannot be null");
        Preconditions.checkNotNull(avroSchema, "schema cannot be null");
        return new WithSchema<>(avroSchema.getSchemaInfo(), object, (x, y) -> object);
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

    /**
     * Create WithSchema object for json. 
     *
     * @param object Object.
     * @param jsonSchema Json Schema for object.
     * @param <T> Type of object. 
     * @return A WithSchema object which has Json schema and the corresponding object.  
     */
    public static <T> WithSchema<T> json(T object, JSONSchema<T> jsonSchema) {
        Preconditions.checkNotNull(object, "object cannot be null");
        Preconditions.checkNotNull(jsonSchema, "schema cannot be null");
        return new WithSchema<>(jsonSchema.getSchemaInfo(), object, (x, y) -> object);
    }
}
