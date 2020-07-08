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
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.IOException;
import java.io.InputStream;

class JsonStringDeserializer extends AbstractDeserializer<String> {
    private final ObjectMapper objectMapper;

    JsonStringDeserializer(String groupId, SchemaRegistryClient client,
                           SerializerConfig.Decoder decoder, EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, null, false, decoder, encodingCache, encodeHeader);
        this.objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY);
    }
    
    @Override
    protected String deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        Object obj = objectMapper.readValue(inputStream, Object.class);
        return objectMapper.writeValueAsString(obj);
    }
}