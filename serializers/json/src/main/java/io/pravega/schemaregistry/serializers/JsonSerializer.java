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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.Encoder;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.JSONSchema;

import java.io.IOException;
import java.io.OutputStream;

class JsonSerializer<T> extends AbstractSerializer<T> {
    private final ObjectMapper objectMapper;
    JsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema<T> schema,
                   Encoder encoder, boolean registerSchema, boolean encodeHeader) {
        super(groupId, client, schema, encoder, registerSchema, encodeHeader);
        objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    }

    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
        objectMapper.writeValue(outputStream, var);
        outputStream.flush();
    }
}
