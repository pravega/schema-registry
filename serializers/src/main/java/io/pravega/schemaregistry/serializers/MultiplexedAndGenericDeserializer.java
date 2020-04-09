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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.InputStream;
import java.util.Map;

class MultiplexedAndGenericDeserializer<T, G> extends AbstractPravegaDeserializer<Either<T, G>> {
    private final Map<String, AbstractPravegaDeserializer<T>> deserializers;
    private final AbstractPravegaDeserializer<G> genericDeserializer;

    MultiplexedAndGenericDeserializer(String groupId, SchemaRegistryClient client,
                                      Map<String, AbstractPravegaDeserializer<T>> deserializers,
                                      AbstractPravegaDeserializer<G> genericDeserializer,
                                      SerializerConfig.Decoder decoder,
                                      EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.deserializers = deserializers; 
        this.genericDeserializer = genericDeserializer;
    }

    @Override
    protected Either<T, G> deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        Preconditions.checkNotNull(writerSchema);
        if (deserializers.containsKey(writerSchema.getName())) {
            return Either.left(deserializers.get(writerSchema.getName()).deserialize(inputStream, writerSchema, readerSchema));
        } else {
            return Either.right(genericDeserializer.deserialize(inputStream, writerSchema, readerSchema));
        }
    }
}