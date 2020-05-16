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
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.RegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.InputStream;
import java.util.Map;

class MultiplexedDeserializer<T> extends AbstractPravegaDeserializer<T> {
    private final Map<String, AbstractPravegaDeserializer<T>> deserializers;

    MultiplexedDeserializer(String groupId, RegistryClient client,
                            Map<String, AbstractPravegaDeserializer<T>> deserializers,
                            SerializerConfig.Decoder decoder, 
                            EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.deserializers = deserializers; 
    }

    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        Preconditions.checkNotNull(writerSchema);
        return deserializers.get(writerSchema.getName()).deserialize(inputStream, writerSchema, readerSchema);
    }
}