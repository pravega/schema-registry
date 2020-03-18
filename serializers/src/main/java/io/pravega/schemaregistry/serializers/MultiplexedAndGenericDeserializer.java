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
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;

class MultiplexedAndGenericDeserializer<T, G> extends AbstractPravegaDeserializer<Either<T, G>> {
    private final Map<String, AbstractPravegaDeserializer<T>> deserializers;
    private final AbstractPravegaDeserializer<G> genericDeserializer;

    MultiplexedAndGenericDeserializer(String groupId, SchemaRegistryClient client,
                                      Map<String, AbstractPravegaDeserializer<T>> deserializers,
                                      AbstractPravegaDeserializer<G> genericDeserializer,
                                      boolean skipHeaders, BiFunction<CodecType, ByteBuffer, ByteBuffer> decode,
                                      EncodingCache encodingCache) {
        super(groupId, client, null, skipHeaders, decode, encodingCache);
        // 1. validate each schema
        // 2. ensure all data 
        this.deserializers = deserializers; 
        this.genericDeserializer = genericDeserializer;
    }

    @Override
    protected Either<T, G> deserialize(ByteBuffer buffer, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        Preconditions.checkNotNull(writerSchema);
        if (deserializers.containsKey(writerSchema.getName())) {
            return Either.left(deserializers.get(writerSchema.getName()).deserialize(buffer, writerSchema, readerSchema));
        } else {
            return Either.right(genericDeserializer.deserialize(buffer, writerSchema, readerSchema));
        }
    }
}