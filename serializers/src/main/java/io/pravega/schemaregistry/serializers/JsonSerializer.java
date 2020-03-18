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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.JSONSchema;
import lombok.SneakyThrows;
import java.io.OutputStream;

class JsonSerializer<T> extends AbstractPravegaSerializer<T> {
    private final ObjectMapper objectMapper;
    JsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema<T> schema,
                   Codec codec, boolean registerSchema, EncodingCache encodingCache) {
        super(groupId, client, schema, codec, registerSchema, encodingCache);
        objectMapper = new ObjectMapper();
    }

    @SneakyThrows
    @Override
    protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) {
        String json = objectMapper.writeValueAsString(var);
        outputStream.write(json.getBytes(Charsets.UTF_8));
        outputStream.flush();
    }
}
