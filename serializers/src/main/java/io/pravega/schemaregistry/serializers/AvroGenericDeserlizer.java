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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;
import java.io.InputStream;

class AvroGenericDeserlizer extends AbstractPravegaDeserializer<GenericRecord> {
    private final LoadingCache<SchemaInfo, Schema> knownSchemas;

    AvroGenericDeserlizer(String groupId, SchemaRegistryClient client, @Nullable AvroSchema<GenericRecord> schema,
                          SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache);
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<SchemaInfo, Schema>() {
            @Override
            public Schema load(SchemaInfo schemaInfo) throws Exception {
                return AvroSchema.from(schemaInfo).getSchema();
            }
        });
    }

    @SneakyThrows
    @Override
    protected GenericRecord deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema = knownSchemas.get(writerSchemaInfo);
        Schema readerSchema = knownSchemas.get(readerSchemaInfo);
        
        GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return genericDatumReader.read(null, decoder);
    }
}
