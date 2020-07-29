/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.protobuf.serializers;

import com.google.protobuf.Message;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.shared.codec.Encoder;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.shared.serializers.AbstractSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class ProtobufSerializer<T extends Message> extends AbstractSerializer<T> {
    public ProtobufSerializer(String groupId, SchemaRegistryClient client, ProtobufSchema<T> schema,
                       Encoder encoder, boolean registerSchema, boolean encodeHeader) {
        super(groupId, client, schema, encoder, registerSchema, encodeHeader);
    }

    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
        var.writeTo(outputStream);
        outputStream.flush();
    }
}
