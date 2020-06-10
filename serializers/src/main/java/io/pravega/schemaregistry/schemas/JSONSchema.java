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

import java.nio.ByteBuffer;

/**
 * Container class for Json Schema.
 * 
 * @param <T> Type of element. 
 */
public class JSONSchema<T> implements SchemaContainer<T> {
    private final String schemaString;
    @Getter
    private final Class<T> tClass;
    @Getter
    private final Class<? extends T> tDerivedClass;
    
    @Getter
    private final JsonSchema schema;

    private final SchemaInfo schemaInfo;
    
    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> tClass) {
        this(schema, name, schemaString, tClass, tClass);
    }
    
    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> tClass, Class<? extends T> tDerivedClass) {
        String type = name != null ? name : schema.getId();
        // Add empty name if the name is not supplied and cannot be extracted from the json schema id. 
        type = type != null ? type : "";
        this.schemaString = schemaString;
        this.schemaInfo = new SchemaInfo(type, SerializationFormat.Json, getSchemaBytes(), ImmutableMap.of());
        this.tClass = tClass;
        this.tDerivedClass = tDerivedClass;
        this.schema = schema;
    }
    
    private JSONSchema(SchemaInfo schemaInfo, JsonSchema schema, String schemaString, Class<T> tClass) {
        this.schemaString = schemaString;
        this.schemaInfo = schemaInfo;
        this.tClass = tClass;
        this.tDerivedClass = tClass;
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
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(objectMapper);
        JsonSchema schema = schemaGen.generateSchema(tClass);
        String schemaString = objectMapper.writeValueAsString(schema);
        
        return new JSONSchema<>(schema, null, schemaString, tClass);    
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
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchema schema = objectMapper.readValue(schemaString, JsonSchema.class);  
        return new JSONSchema<>(schema, type, schemaString, Object.class);
    }
    
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static <T> JSONSchema<T> ofBaseType(Class<? extends T> tDerivedClass, Class<T> tClass) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(objectMapper);
        JsonSchema schema = schemaGen.generateSchema(tDerivedClass);
        String schemaString = objectMapper.writeValueAsString(schema);

        return new JSONSchema<>(schema, null, schemaString, tClass, tDerivedClass);
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     *
     * @param schemaInfo Schema info to translate into json schema. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static JSONSchema<Object> from(SchemaInfo schemaInfo) {
        ObjectMapper objectMapper = new ObjectMapper();
        String schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
        
        JsonSchema schema = objectMapper.readValue(schemaString, JsonSchema.class);
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
