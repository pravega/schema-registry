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
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

public class AvroSchema<T> implements SchemaData<T> {
    @Getter
    private final Schema schema;
    private final SchemaInfo schemaInfo;
    private final Class<T> tClass;
    
    private AvroSchema(Schema schema, Class<T> tClass) {
        this.schema = schema;
        this.schemaInfo = new SchemaInfo(
                schema.getName(),
                SchemaType.Avro, getSchemaBytes(), ImmutableMap.of());
        this.tClass = tClass;
    }

    public static <T> AvroSchema<T> of(Class<T> tClass) {
        Schema schema;
        if (tClass.isAssignableFrom(SpecificRecordBase.class)) {
            schema = SpecificData.get().getSchema(tClass);
        } else {
            schema = ReflectData.get().getSchema(tClass);
        }
        return new AvroSchema<>(schema, tClass);
    }
    
    public static <T> AvroSchema<T> of(Class<T> tClass, Schema schema) {
        return new AvroSchema<>(schema, tClass);
    }
    
    public static AvroSchema<GenericRecord> of(Schema schema) {
        return new AvroSchema<>(schema, null);
    }
    
    private byte[] getSchemaBytes() {
        return schema.toString().getBytes(Charsets.UTF_8);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
