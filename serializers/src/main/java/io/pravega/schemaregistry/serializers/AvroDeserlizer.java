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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.AvroSchema;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.InputStream;

class AvroDeserlizer<T extends IndexedRecord> extends AbstractPravegaDeserializer<T> {
    private final AvroSchema<T> avroSchema;
    private final LoadingCache<byte[], Schema> knownSchemas;

    AvroDeserlizer(String groupId, SchemaRegistryClient client,
                   AvroSchema<T> schema,
                   SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache);
        Preconditions.checkNotNull(schema);
        this.avroSchema = schema;
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<byte[], Schema>() {
            @Override
            public Schema load(byte[] schemaData) throws Exception {
                String schemaString = new String(schemaData, Charsets.UTF_8);
                return new Schema.Parser().parse(schemaString);
            }
        });
    }

    @SneakyThrows
    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema = knownSchemas.get(writerSchemaInfo.getSchemaData().array());
        Schema readerSchema = avroSchema.getSchema();
        
        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return datumReader.read(null, decoder);
    }
}
