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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

class AvroGenericDeserlizer extends AbstractPravegaDeserializer<GenericRecord> {
    private final LoadingCache<String, Schema> knownSchemas;

    AvroGenericDeserlizer(String groupId, SchemaRegistryClient client, @Nullable AvroSchema<GenericRecord> schema,
                          BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress, EncodingCache encodingCache) {
        super(groupId, client, schema, false, uncompress, encodingCache);
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<String, Schema>() {
            @Override
            public Schema load(String schemaString) throws Exception {
                return new Schema.Parser().parse(schemaString);
            }
        });
    }

    @SneakyThrows
    @Override
    protected GenericRecord deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema = knownSchemas.get(new String(writerSchemaInfo.getSchemaData(), Charsets.UTF_8));
        Schema readerSchema = knownSchemas.get(new String(readerSchemaInfo.getSchemaData(), Charsets.UTF_8));
        
        GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(array, null);
        return genericDatumReader.read(null, decoder);
    }
}
