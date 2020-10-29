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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.tuple.Pair;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

public class AvroGenericDeserializer extends AbstractDeserializer<Object> {
    private final ConcurrentHashMap<Pair<SchemaInfo, SchemaInfo>, GenericDatumReader<Object>> knownSchemaReaders;

    public AvroGenericDeserializer(String groupId, SchemaRegistryClient client, @Nullable AvroSchema<Object> schema,
                            SerializerConfig.Decoders decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache, true);
        this.knownSchemaReaders = new ConcurrentHashMap<>();
    }

    @Override
    public final Object deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        Preconditions.checkNotNull(writerSchemaInfo);
        final Pair<SchemaInfo, SchemaInfo> keyPair = Pair.of(writerSchemaInfo, readerSchemaInfo);
        GenericDatumReader<Object> genericDatumReader = knownSchemaReaders.computeIfAbsent(keyPair, key -> {
            Schema writerSchema = AvroSchema.from(writerSchemaInfo).getSchema();
            Schema readerSchema = AvroSchema.from(readerSchemaInfo).getSchema();
            return new GenericDatumReader<>(writerSchema, readerSchema);
        });
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return genericDatumReader.read(null, decoder);
    }

    @VisibleForTesting
    ImmutableMap<Pair<SchemaInfo, SchemaInfo>, GenericDatumReader<Object>> getKnownSchemaReaders() {
        return ImmutableMap.copyOf(knownSchemaReaders);
    }
}
