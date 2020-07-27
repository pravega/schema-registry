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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

class AvroDeserializer<T> extends AbstractDeserializer<T> {
    private final AvroSchema<T> avroSchema;
    private final ConcurrentHashMap<SchemaInfo, Schema> knownSchemas;

    AvroDeserializer(String groupId, SchemaRegistryClient client,
                     AvroSchema<T> schema,
                     SerializerConfig.Decoders decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache, true);
        Preconditions.checkNotNull(schema);
        this.avroSchema = schema;
        this.knownSchemas = new ConcurrentHashMap<>();
    }

    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema;
        if (knownSchemas.containsKey(writerSchemaInfo)) {
            writerSchema = knownSchemas.get(writerSchemaInfo);
        } else {
            String schemaString = new String(writerSchemaInfo.getSchemaData().array(), Charsets.UTF_8);
            writerSchema = new Schema.Parser().parse(schemaString);
            knownSchemas.put(writerSchemaInfo, writerSchema);
        }
        Schema readerSchema = avroSchema.getSchema();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        
        if (SpecificRecordBase.class.isAssignableFrom(avroSchema.getTClass())) {
            SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
            return datumReader.read(null, decoder);
        } else {
            ReflectDatumReader<T> datumReader = new ReflectDatumReader<>(writerSchema, readerSchema);
            return datumReader.read(null, decoder);
        }
    }
}
