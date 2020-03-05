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

import com.google.protobuf.GeneratedMessageV3;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializers.AbstractPravegaSerializer;
import lombok.SneakyThrows;
import java.io.OutputStream;

public class ProtobufSerializer<T extends GeneratedMessageV3> extends AbstractPravegaSerializer<T> {
    public ProtobufSerializer(String groupId, SchemaRegistryClient client, ProtobufSchema<T> schema,
                              Compressor compressor, boolean registerSchema, EncodingCache encodingCache) {
        super(groupId, client, schema, compressor, registerSchema, encodingCache);
    }

    @SneakyThrows
    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) {
        var.writeTo(outputStream);
    }
}
