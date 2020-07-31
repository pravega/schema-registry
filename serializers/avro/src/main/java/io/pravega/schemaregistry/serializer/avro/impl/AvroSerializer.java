/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.avro.impl;

import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.serializer.shared.codec.Encoder;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.OutputStream;

public class AvroSerializer<T> extends AbstractSerializer<T> {
    private final AvroSchema<T> avroSchema;
    public AvroSerializer(String groupId, SchemaRegistryClient client, AvroSchema<T> schema,
                   Encoder encoder, boolean registerSchema) {
        super(groupId, client, schema, encoder, registerSchema, true);
        this.avroSchema = schema;
    }

    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
        Schema schema = avroSchema.getSchema();
        
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

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
        outputStream.flush();
    }
}
