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
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.Getter;
import lombok.SneakyThrows;

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
    
    private JSONSchema(JsonSchema schema, String schemaString, Class<T> tClass) {
        this(schema, schemaString, tClass, tClass);
    }
    
    private JSONSchema(JsonSchema schema, String schemaString, Class<T> tClass, Class<? extends T> tDerivedClass) {
        this.schemaString = schemaString;
        String schemaId = schema.getId() == null ? "" : schema.getId();
        this.schemaInfo = new SchemaInfo(
                schemaId,
                SchemaType.Json, getSchemaBytes(), ImmutableMap.of());
        this.tClass = tClass;
        this.tDerivedClass = tDerivedClass;
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
        
        return new JSONSchema<>(schema, schemaString, tClass);    
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     *
     * @param schemaString Schema string to use. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static JSONSchema<Object> of(String schemaString) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchema schema = objectMapper.readValue(schemaString, JsonSchema.class);  
        return new JSONSchema<>(schema, schemaString, Object.class);
    }
    
    @SneakyThrows({JsonMappingException.class, JsonProcessingException.class})
    public static <T> JSONSchema<T> ofBaseType(Class<? extends T> tDerivedClass, Class<T> tClass) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(objectMapper);
        JsonSchema schema = schemaGen.generateSchema(tDerivedClass);
        String schemaString = objectMapper.writeValueAsString(schema);

        return new JSONSchema<>(schema, schemaString, tClass, tDerivedClass);
    }

    private byte[] getSchemaBytes() {
        return schemaString.getBytes(Charsets.UTF_8);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
