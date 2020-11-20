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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

class AvroDeserializer<T> extends AbstractDeserializer<T> {
    private final ConcurrentHashMap<ByteBuffer, DatumReader<T>> knownSchemaReaders;
    private final boolean specific;
    private final Schema readerSchema;

    AvroDeserializer(String groupId, SchemaRegistryClient client,
                     AvroSchema<T> schema,
                     SerializerConfig.Decoders decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache, true);
        Preconditions.checkNotNull(schema);
        this.knownSchemaReaders = new ConcurrentHashMap<>();
        specific = SpecificRecordBase.class.isAssignableFrom(schema.getTClass());
        readerSchema = schema.getSchema();
        ByteBuffer schemaData = schema.getSchemaInfo().getSchemaData();
        knownSchemaReaders.put(schemaData, createDatumReader(readerSchema, readerSchema, specific));
    }

    @Override
    public final T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        Preconditions.checkNotNull(writerSchemaInfo);
        final ByteBuffer writerSchemaData = writerSchemaInfo.getSchemaData();
        DatumReader<T> datumReader = knownSchemaReaders.computeIfAbsent(writerSchemaData, key -> {
            Schema writerSchema = AvroSchema.from(writerSchemaInfo).getSchema();
            return createDatumReader(writerSchema, this.readerSchema, specific);
        });
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return datumReader.read(null, decoder);
    }

    @VisibleForTesting
     DatumReader<T> createDatumReader(Schema writerSchema, Schema readerSchema, boolean specific) {
        DatumReader<T> datumReader;
        if (specific) {
            datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
        } else {
            datumReader = new ReflectDatumReader<>(writerSchema, readerSchema);
        }
        return datumReader;
    }

    @VisibleForTesting
    ImmutableMap<ByteBuffer, DatumReader<T>> getKnownSchemaReaders() {
        return ImmutableMap.copyOf(knownSchemaReaders);
    }
}
