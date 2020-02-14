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

import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

import java.util.Collections;

public class AvroSchema<T> implements SchemaData<T>{
    @Getter
    private final Schema schema;
    private final SchemaRegistryContract.SchemaInfo schemaInfo;

    private AvroSchema(Schema schema) {
        this.schema = schema;
        this.schemaInfo = new SchemaRegistryContract.SchemaInfo(
                schema.getName(),
                SchemaRegistryContract.SchemaType.Avro, getSchemaBytes(), Collections.emptyMap());
    }

    static <T> AvroSchema<T> of(Class<T> tClass) {
        Schema schema;
        if (tClass.isAssignableFrom(GenericRecord.class)) {
            schema = SpecificData.get().getSchema(tClass);
        } else {
            schema = ReflectData.get().getSchema(tClass);
        }
        return new AvroSchema<>(schema);
    }
    
    static <T> AvroSchema<T> of(Schema schema) {
        return new AvroSchema<>(schema);
    }
    
    @Override
    public byte[] getSchemaBytes() {
        return schema.toString().getBytes();
    }

    @Override
    public SchemaRegistryContract.SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public SchemaRegistryContract.SchemaType getSchemaType() {
        return schemaInfo.getSchemaType();
    }
}
