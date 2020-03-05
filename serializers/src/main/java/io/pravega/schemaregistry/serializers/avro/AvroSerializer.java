/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers.avro;

import com.google.common.base.Charsets;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializers.AbstractPravegaSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class AvroSerializer<T> extends AbstractPravegaSerializer<T> {
    AvroSchema<T> avroSchema;
    public AvroSerializer(String groupId, SchemaRegistryClient client, AvroSchema<T> schema,
                          Compressor compressor, boolean registerSchema, EncodingCache encodingCache) {
        super(groupId, client, schema, compressor, registerSchema, encodingCache);
        this.avroSchema = schema;
    }

    @SneakyThrows
    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) {
        Schema schema;
        Schema.Parser parser = new Schema.Parser();
        if (avroSchema == null) {
            schema = parser.parse(new String(schemaInfo.getSchemaData(), Charsets.UTF_8));
        } else {
            schema = avroSchema.getSchema();
        }
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        if (var.getClass().isAssignableFrom(GenericRecord.class)) {
            if (var.getClass().isAssignableFrom(SpecificRecord.class)) {
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
