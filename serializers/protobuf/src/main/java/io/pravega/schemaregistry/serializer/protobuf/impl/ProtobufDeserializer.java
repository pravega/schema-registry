/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.protobuf.impl;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;

import java.io.IOException;
import java.io.InputStream;

public class ProtobufDeserializer<T extends GeneratedMessageV3> extends AbstractDeserializer<T> {
    private final ProtobufSchema<T> protobufSchema;
    ProtobufDeserializer(String groupId, SchemaRegistryClient client,
                         ProtobufSchema<T> schema, SerializerConfig.Decoders decoder,
                         EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, schema, true, decoder, encodingCache, encodeHeader, canCloseClient);
        Preconditions.checkNotNull(schema);
        this.protobufSchema = schema;
    }

    @Override
    public final T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        try {
            return protobufSchema.getParser().parseFrom(inputStream);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Invalid protobuffer serialized bytes", e);
        }
    }
}
