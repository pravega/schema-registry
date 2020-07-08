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
import org.apache.commons.lang3.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Deserializer which multiplexes for multiple object types. Based on the supplied object, it invokes the 
 * deserializer for that object type.
 * 
 * @param <T> Type of object.
 */
class MultiplexedDeserializer<T> extends AbstractDeserializer<T> {
    private final Map<String, AbstractDeserializer<T>> deserializers;

    MultiplexedDeserializer(String groupId, SchemaRegistryClient client,
                            Map<String, AbstractDeserializer<T>> deserializers,
                            SerializerConfig.Decoder decoder, 
                            EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache, true);
        this.deserializers = deserializers; 
    }

    @Override
    protected T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException {
        Preconditions.checkNotNull(writerSchema);
        AbstractDeserializer<T> deserializer = deserializers.get(writerSchema.getType());
        if (deserializer == null) {
            throw new SerializationException("deserializer not supplied for type " + writerSchema.getType());
        }
        return deserializer.deserialize(inputStream, writerSchema, readerSchema);
    }
}