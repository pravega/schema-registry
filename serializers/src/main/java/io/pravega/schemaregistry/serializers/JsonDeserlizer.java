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

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.JSONSchema;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

class JsonDeserlizer<T> extends AbstractPravegaDeserializer<T> {
    private final JSONSchema<T> jsonSchema;
    private final ObjectMapper objectMapper;

    JsonDeserlizer(String groupId, SchemaRegistryClient client,
                   JSONSchema<T> schema,
                   SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, true, decoder, encodingCache);
        Preconditions.checkNotNull(schema);
        this.jsonSchema = schema;
        this.objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.CREATOR, Visibility.ANY);
    }

    @SneakyThrows({JsonProcessingException.class, IOException.class})
    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        return objectMapper.readValue(inputStream, jsonSchema.getTClass());
    }
}
