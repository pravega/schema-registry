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
import com.google.protobuf.GeneratedMessageV3;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public class ProtobufDeserlizer<T extends GeneratedMessageV3> extends AbstractPravegaDeserializer<T> {
    private final ProtobufSchema<T> protobufSchema;
    ProtobufDeserlizer(String groupId, SchemaRegistryClient client,
                       ProtobufSchema<T> schema, SerializerConfig.Decoder decoder,
                       boolean failOnCodecMismatch,
                       EncodingCache encodingCache) {
        super(groupId, client, schema, true, decoder, failOnCodecMismatch, encodingCache);
        Preconditions.checkNotNull(schema);
        this.protobufSchema = schema;
    }

    @SneakyThrows
    @Override
    protected T deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        return protobufSchema.getParser().parseFrom(buffer);
    }
}
