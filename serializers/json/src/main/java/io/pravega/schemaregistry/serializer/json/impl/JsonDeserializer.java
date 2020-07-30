/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.json.impl;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;

import java.io.IOException;
import java.io.InputStream;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

public class JsonDeserializer<T> extends AbstractDeserializer<T> {
    private final JSONSchema<T> jsonSchema;
    private final ObjectMapper objectMapper;

    public JsonDeserializer(String groupId, SchemaRegistryClient client,
                     JSONSchema<T> schema,
                     SerializerConfig.Decoders decoders, EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, schema, true, decoders, encodingCache, encodeHeader, canCloseClient);
        Preconditions.checkNotNull(schema);
        this.jsonSchema = schema;
        this.objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY);
    }

    @Override
    public final T deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        return objectMapper.readValue(inputStream, jsonSchema.getDerived());
    }
}
