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

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.BiFunction;

class MultiFormatWithSchemaDeserializer<T> extends AbstractDeserializer<WithSchema<T>> {
    private final Map<SerializationFormat, AbstractDeserializer> genericDeserializers;
    private final BiFunction<SerializationFormat, Object, T> transform;

    MultiFormatWithSchemaDeserializer(String groupId, SchemaRegistryClient client,
                                      Map<SerializationFormat, AbstractDeserializer> genericDeserializers,
                                      SerializerConfig.Decoders decoders,
                                      EncodingCache encodingCache, BiFunction<SerializationFormat, Object, T> transform) {
        super(groupId, client, null, false, decoders, encodingCache, true);
        this.genericDeserializers = genericDeserializers;
        this.transform = transform;
    }

    @Override
    protected WithSchema<T> deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException {
        Preconditions.checkNotNull(writerSchema);
        Object obj = genericDeserializers.get(writerSchema.getSerializationFormat()).deserialize(inputStream, writerSchema, readerSchema);
        if (obj instanceof WithSchema) {
            obj = ((WithSchema) obj).getObject();
        }
        return new WithSchema<>(writerSchema, obj, transform);
    }
}