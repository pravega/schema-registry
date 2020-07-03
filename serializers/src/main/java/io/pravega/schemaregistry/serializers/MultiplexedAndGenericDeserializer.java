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
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

class MultiplexedAndGenericDeserializer<T, G> extends AbstractDeserializer<Either<T, G>> {
    private final Map<String, AbstractDeserializer<T>> deserializers;
    private final AbstractDeserializer<G> genericDeserializer;

    MultiplexedAndGenericDeserializer(String groupId, SchemaRegistryClient client,
                                      Map<String, AbstractDeserializer<T>> deserializers,
                                      AbstractDeserializer<G> genericDeserializer,
                                      SerializerConfig.Decoder decoder,
                                      EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.deserializers = deserializers; 
        this.genericDeserializer = genericDeserializer;
    }

    @Override
    protected Either<T, G> deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException {
        Preconditions.checkNotNull(writerSchema);
        AbstractDeserializer<T> deserializer = deserializers.get(writerSchema.getType());
        if (deserializer == null) {
            return Either.right(genericDeserializer.deserialize(inputStream, writerSchema, readerSchema));
        } else {
            return Either.left(deserializers.get(writerSchema.getType()).deserialize(inputStream, writerSchema, readerSchema));
        } 
    }
}