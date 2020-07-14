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
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.Getter;
import org.apache.avro.specific.SpecificRecordBase;

import java.nio.ByteBuffer;

/**
 * Container class for Json Schema.
 *
 * @param <T> Type of element. 
 */
public class JSONSchema<T> implements Schema<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Getter
    private final String schemaString;
    private final Class<T> base;
    @Getter
    private final Class<? extends T> derived;
    
    @Getter
    private final JsonSchema schema;

    private final SchemaInfo schemaInfo;

    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> derived) {
        this(schema, name, schemaString, derived, derived);
    }

    private JSONSchema(JsonSchema schema, String name, String schemaString, Class<T> base, Class<? extends T> derived) {
        this.schemaString = schemaString;
        this.schemaInfo = new SchemaInfo(name, SerializationFormat.Json, getSchemaBytes(), ImmutableMap.of());
        this.base = base;
        this.derived = derived;
        this.schema = schema;
    }

    private JSONSchema(SchemaInfo schemaInfo, JsonSchema schema, String schemaString, Class<T> derived) {
        this.schemaString = schemaString;
        this.schemaInfo = schemaInfo;
        this.base = derived;
        this.derived = derived;
        this.schema = schema;
    }

    /**
     * Method to create a typed JSONSchema for the given class. It extracts the json schema from the class.
     * For POJOs the schema is extracted using jackson's {@link JsonSchemaGenerator}. 
     *
     * @param tClass Class whose object's schema is used.
     * @param <T> Type of the Java class. 
     * @return {@link JSONSchema} with generic type T that extracts and captures the json schema. 
     */
    public static <T> JSONSchema<T> of(Class<T> tClass) {
        Preconditions.checkNotNull(tClass);
        try {
            JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(OBJECT_MAPPER);
            JsonSchema schema = schemaGen.generateSchema(tClass);
            String schemaString = OBJECT_MAPPER.writeValueAsString(schema);

            return new JSONSchema<>(schema, tClass.getName(), schemaString, tClass);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to get json schema from the class", e);
        }
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     * This method can be used to pass Json schema string which can be used to represent primitive data types. 
     *
     * @param type type of object identified by {@link SchemaInfo#getType()}.
     * @param schema Schema to use. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    public static JSONSchema<Object> of(String type, JsonSchema schema) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(schema);
        try {
            String schemaString = OBJECT_MAPPER.writeValueAsString(schema);

            return new JSONSchema<>(schema, type, schemaString, Object.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to get json schema string from the JsonSchema object", e);
        }
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema string. 
     *
     * @param type type of object identified by {@link SchemaInfo#getType()}.
     * @param schemaString Schema string to use. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    public static JSONSchema<Object> of(String type, String schemaString) {
        Preconditions.checkNotNull(type, "Type cannot be null.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaString), "Schema String cannot be null or empty.");
        try {
            JsonSchema schema = OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
            return new JSONSchema<>(schema, type, schemaString, Object.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to parse schema string", e);
        }
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
    public static <T> JSONSchema<T> ofBaseType(Class<? extends T> tDerived, Class<T> tBase) {
        Preconditions.checkNotNull(tDerived);
        Preconditions.checkNotNull(tBase);
        try {
            JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(OBJECT_MAPPER);
            JsonSchema schema = schemaGen.generateSchema(tDerived);
            String schemaString = OBJECT_MAPPER.writeValueAsString(schema);

            return new JSONSchema<>(schema, tDerived.getName(), schemaString, tBase, tDerived);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to get json schema from the class", e);
        }
    }

    /**
     * Method to create a typed JSONSchema of type {@link Object} from the given schema. 
     *
     * @param schemaInfo Schema info to translate into json schema. 
     * @return Returns an JSONSchema with {@link Object} type. 
     */
    public static JSONSchema<Object> from(SchemaInfo schemaInfo) {
        Preconditions.checkNotNull(schemaInfo);
        try {
            String schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);

            JsonSchema schema = OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
            return new JSONSchema<>(schemaInfo, schema, schemaString, Object.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to get json schema from schema info", e);
        }
    }

    private ByteBuffer getSchemaBytes() {
        return ByteBuffer.wrap(schemaString.getBytes(Charsets.UTF_8));
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Class<T> getTClass() {
        return base;
    }
}
