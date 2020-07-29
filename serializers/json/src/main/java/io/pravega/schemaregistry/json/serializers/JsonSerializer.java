/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.json.serializers;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.shared.codec.Encoder;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.json.schemas.JSONSchema;
import io.pravega.schemaregistry.shared.serializers.AbstractSerializer;

import java.io.IOException;
import java.io.OutputStream;

public class JsonSerializer<T> extends AbstractSerializer<T> {
    private final ObjectMapper objectMapper;
    public JsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema<T> schema,
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
