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

import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

class AvroSerializer<T> extends AbstractPravegaSerializer<T> {
    private final AvroSchema<T> avroSchema;
    AvroSerializer(String groupId, RegistryClient client, AvroSchema<T> schema,
                   Codec codec, boolean registerSchema) {
        super(groupId, client, schema, codec, registerSchema);
        this.avroSchema = schema;
    }

    @SneakyThrows
    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) {
        Schema schema = avroSchema.getSchema();
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        if (IndexedRecord.class.isAssignableFrom(var.getClass())) {
            if (SpecificRecord.class.isAssignableFrom(var.getClass())) {
                SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
                writer.write(var, encoder);
            } else {
                GenericDatumWriter<T> writer = new GenericDatumWriter<>(schema);
                writer.write(var, encoder);
            }
        } else {
            ReflectDatumWriter<T> writer = new ReflectDatumWriter<>(schema);
            writer.write(var, encoder);
        }

        encoder.flush();
        outputStream.write(out.toByteArray());
        outputStream.flush();
    }
}
