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

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.schemas.AvroSchema;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.serializers.AbstractPravegaDeserializer;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;

public class PravegaAvroDeserlizer<T> extends AbstractPravegaDeserializer<T> {
    AvroSchema<T> avroSchema;
    public PravegaAvroDeserlizer(String scope, String groupId, SchemaRegistryClient client,
                                    @Nullable AvroSchema<T> schema,
                                    SerializerConfig config,
                                    BiFunction<ByteBuffer, CompressionType, ByteBuffer> uncompress) {
        super(scope, groupId, client, schema, config, false, uncompress);
        this.avroSchema = schema;
    }

    @SneakyThrows
    @Override
    protected T deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkNotNull(writerSchemaInfo);
        Schema writerSchema = Schema.parse(new String(writerSchemaInfo.getSchemaData()));
        Schema readerSchema;
        if (avroSchema == null) {
            if (readerSchemaInfo == null) {
                // read using writer schema
                readerSchema = writerSchema;
            } else {
                readerSchema = Schema.parse(new String(readerSchemaInfo.getSchemaData()));
            }
        } else {
            readerSchema = avroSchema.getSchema();
        }
        
        GenericDatumReader<T> genericDatumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder _decoder = DecoderFactory.get().binaryDecoder(buffer.array(), null);
        return genericDatumReader.read(null, _decoder);
    }
}
