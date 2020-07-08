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

class MultipleFormatDeserializer<T> extends AbstractDeserializer<T> {
    private final Map<SerializationFormat, AbstractDeserializer> genericDeserializers;
    private final BiFunction<SerializationFormat, Object, T> transform;

    MultipleFormatDeserializer(String groupId, SchemaRegistryClient client,
                               Map<SerializationFormat, AbstractDeserializer> genericDeserializers,
                               SerializerConfig.Decoder decoder,
                               EncodingCache encodingCache, BiFunction<SerializationFormat, Object, T> transform) {
        super(groupId, client, null, false, decoder, encodingCache, true);
        this.genericDeserializers = genericDeserializers;
        this.transform = transform;
    }

    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException {
        Preconditions.checkNotNull(writerSchema);
        return transform.apply(writerSchema.getSerializationFormat(), 
                genericDeserializers.get(writerSchema.getSerializationFormat()).deserialize(inputStream, writerSchema, readerSchema));
    }
}