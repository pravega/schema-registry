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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import java.nio.ByteBuffer;

/**
 * Container class for Avro Schema.
 *
 * @param <T> Type of element. 
 */
public class AvroSchema<T> implements SchemaContainer<T> {
    @Getter
    private final Schema schema;
    private final SchemaInfo schemaInfo;

    private AvroSchema(Schema schema) {
        this.schema = schema;
        this.schemaInfo = new SchemaInfo(schema.getName(),
                SerializationFormat.Avro, getSchemaBytes(), ImmutableMap.of());
    }

    private AvroSchema(SchemaInfo schemaInfo) {
        String schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
        this.schema = new Schema.Parser().parse(schemaString);
        this.schemaInfo = schemaInfo;
    }

    /**
     * Method to create a typed AvroSchema for the given class. It extracts the avro schema from the class.
     * For Avro generated classes, the schema is retrieved from the class. 
     * For POJOs the schema is extracted using avro's {@link ReflectData}. 
     *
     * @param tClass Class whose object's schema is used.
     * @param <T> Type of the Java class. 
     * @return {@link AvroSchema} with generic type T that extracts and captures the avro schema. 
     */
    public static <T> AvroSchema<T> of(Class<T> tClass) {
        Schema schema;
        if (SpecificRecordBase.class.isAssignableFrom(tClass)) {
            schema = SpecificData.get().getSchema(tClass);
        } else {
            schema = ReflectData.get().getSchema(tClass);
        }
        return new AvroSchema<>(schema);
    }

    /**
     * Method to create a typed AvroSchema of type {@link GenericRecord} from the given schema. 
     *
     * @param schema Schema to use. 
     * @return Returns an AvroSchema with {@link GenericRecord} type. 
     */
    public static AvroSchema<GenericRecord> of(Schema schema) {
        return new AvroSchema<>(schema);
    }

    /**
     * It is same as {@link #of(Class)} except that it generates an AvroSchema typed as {@link SpecificRecordBase}. 
     *
     * This is useful for supplying a map of Avro schemas for multiplexed serializers and deserializers. 
     *
     * @param tClass Class whose schema should be used.
     * @param <T> Type of class whose schema is to be used. 
     * @return Returns an AvroSchema with {@link SpecificRecordBase} type. 
     */
    public static <T extends SpecificRecordBase> AvroSchema<T> ofBaseType(Class<? extends T> tClass) {
        Preconditions.checkArgument(SpecificRecordBase.class.isAssignableFrom(tClass));

        return new AvroSchema<>(SpecificData.get().getSchema(tClass));
    }

    /**
     * Method to create a typed AvroSchema of type {@link GenericRecord} from schema info. 
     *
     * @param schemaInfo Schema info object that has schema data in binary form.  
     * @return Returns an AvroSchema with {@link GenericRecord} type. 
     */
    public static AvroSchema<GenericRecord> from(SchemaInfo schemaInfo) {
        return new AvroSchema<>(schemaInfo);
    }

    private ByteBuffer getSchemaBytes() {
        return ByteBuffer.wrap(schema.toString().getBytes(Charsets.UTF_8));
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
