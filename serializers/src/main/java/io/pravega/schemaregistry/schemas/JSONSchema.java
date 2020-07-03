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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecordBase;

import java.nio.ByteBuffer;

/**
 * Container class for Json Schema.
 *
 * @param <T> Type of element. 
 */
public class JSONSchema<T> implements SchemaContainer<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Getter
    private final String schemaString;
    private final Class<T> base;
    @Getter
    private final Class<? extends T> tClass;

    @Getter
    private final JsonSchema schema;

    private final SchemaInfo schemaInfo;

    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> tClass) {
        this(schema, name, schemaString, tClass, tClass);
    }

    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> base, Class<? extends T> derived) {
        String type = name != null ? name : schema.getId();
        // Add empty name if the name is not supplied and cannot be extracted from the json schema id. 
        type = type != null ? type : "";
        this.schemaString = schemaString;
        this.schemaInfo = new SchemaInfo(type, SerializationFormat.Json, getSchemaBytes(), ImmutableMap.of());
        this.base = base;
        this.tClass = derived;
        this.schema = schema;
    }

    private JSONSchema(SchemaInfo schemaInfo, JsonSchema schema, String schemaString, Class<T> tClass) {
        this.schemaString = schemaString;
        this.schemaInfo = schemaInfo;
        this.base = tClass;
        this.tClass = tClass;
        this.schema = schema;
    }

    /**
     * Method to create a typed JSONSchema for the given class. It extracts the json schema from the class.
     * For POJOs the schema is extracted using jacksons {@link JsonSchemaGenerator}. 
     *
     * @param tClass Class whose object's schema is used.
     * @param <T> Type of the Java class. 
     * @return {@link JSONSchema} with generic type T that extracts and captures the json schema. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static <T> JSONSchema<T> of(Class<T> tClass) {
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(OBJECT_MAPPER);
        JsonSchema schema = schemaGen.generateSchema(tClass);
        String schemaString = OBJECT_MAPPER.writeValueAsString(schema);

        return new JSONSchema<>(schema, tClass.getName(), schemaString, tClass);
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     *
     * @param type type of object identified by {@link SchemaInfo#type}.
     * @param schemaString Schema string to use. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static JSONSchema<Object> of(String type, String schemaString) {
        JsonSchema schema = OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
        return new JSONSchema<>(schema, type, schemaString, Object.class);
    }

    /**
     * It is same as {@link #of(Class)} except that it generates an JSONSchema typed as supplied base type T. 
     *
     * This is useful for supplying a map of POJO schemas for multiplexed serializers and deserializers. 
     *
     * @param tBase Base class whose type is used in the JSON schema object.
     * @param tDerived Class whose schema should be used.
     * @param <T> Type of base class. 
     * @return Returns an AvroSchema with {@link SpecificRecordBase} type. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static <T> JSONSchema<T> ofBaseType(Class<? extends T> tDerived, Class<T> tBase) {
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(OBJECT_MAPPER);
        JsonSchema schema = schemaGen.generateSchema(tDerived);
        String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
        
        return new JSONSchema<>(schema, tDerived.getName(), schemaString, tBase, tDerived);
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     *
     * @param schemaInfo Schema info to translate into json schema. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static JSONSchema<Object> from(SchemaInfo schemaInfo) {
        String schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);

        JsonSchema schema = OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
        return new JSONSchema<>(schemaInfo, schema, schemaString, Object.class);
    }

    private ByteBuffer getSchemaBytes() {
        return ByteBuffer.wrap(schemaString.getBytes(Charsets.UTF_8));
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
