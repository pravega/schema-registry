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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;

import java.nio.ByteBuffer;
import java.util.Map;

class MultipleFormatGenericDeserializer extends AbstractPravegaDeserializer<Object> {
    private final Map<SchemaType, AbstractPravegaDeserializer> genericDeserializers;

    MultipleFormatGenericDeserializer(String groupId, SchemaRegistryClient client,
                                      Map<SchemaType, AbstractPravegaDeserializer> genericDeserializers,
                                      SerializerConfig.Decoder decoder,
                                      EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.genericDeserializers = genericDeserializers;
    }

    @Override
    protected Object deserialize(ByteBuffer buffer, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        Preconditions.checkNotNull(writerSchema);
        return genericDeserializers.get(writerSchema.getSchemaType()).deserialize(buffer, writerSchema, readerSchema); 
    }
}