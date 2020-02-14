/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers.protobuf;

import com.google.protobuf.Message;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.AbstractPravegaDeserializer;
import io.pravega.schemaregistry.serializers.SerializerConfig;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;

public class PravegaProtobufDeserlizer<T extends Message> extends AbstractPravegaDeserializer<T> {
    ProtobufSchema<T> protobufSchema;
    public PravegaProtobufDeserlizer(String scope, String groupId, SchemaRegistryClient client,
                                     @Nullable ProtobufSchema<T> schema,
                                     SerializerConfig config,
                                     BiFunction<ByteBuffer, CompressionType, ByteBuffer> uncompress) {
        super(scope, groupId, client, schema, config, false, uncompress);
        this.protobufSchema = schema;
    }

    @SneakyThrows
    @Override
    protected T deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        return protobufSchema.getParser().parseFrom(buffer);
    }
}
