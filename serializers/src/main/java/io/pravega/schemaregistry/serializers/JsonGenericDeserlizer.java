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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.JSONSchema;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

class JsonGenericDeserlizer extends AbstractPravegaDeserializer<JsonGenericObject> {
    private final ObjectMapper objectMapper;
    private final LoadingCache<SchemaInfo, JsonSchema> knownSchemas;

    JsonGenericDeserlizer(String groupId, SchemaRegistryClient client,
                          SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<SchemaInfo, JsonSchema>() {
            @Override
            public JsonSchema load(SchemaInfo schemaInfo) throws Exception {
                return JSONSchema.from(schemaInfo).getSchema();
            }
        });
    }
    
    @SneakyThrows({JsonProcessingException.class, ExecutionException.class, IOException.class})
    @Override
    protected JsonGenericObject deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Map obj = objectMapper.readValue(inputStream, Map.class);
        JsonSchema schema = writerSchemaInfo == null ? null : knownSchemas.get(writerSchemaInfo);
        return new JsonGenericObject(obj, schema);
    }
}
