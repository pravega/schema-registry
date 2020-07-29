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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.shared.serializers.AbstractDeserializer;
import io.pravega.schemaregistry.shared.serializers.EncodingCache;
import io.pravega.schemaregistry.shared.serializers.SerializerConfig;

import java.io.IOException;
import java.io.InputStream;

public class JsonGenericDeserializer extends AbstractDeserializer<JsonNode> {
    private final ObjectMapper objectMapper;

    public JsonGenericDeserializer(String groupId, SchemaRegistryClient client,
                            SerializerConfig.Decoders decoders, EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, null, false, decoders, encodingCache, encodeHeader);
        this.objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    }
    
    @Override
    public final JsonNode deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        return objectMapper.readTree(inputStream);
    }
}
