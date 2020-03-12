/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class IndexKeySerializer extends VersionedSerializer.MultiType<IndexRecord.IndexKey> {

    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(IndexRecord.VersionInfoKey.class, 1, new IndexRecord.VersionInfoKey.Serializer())
               .serializer(IndexRecord.ValidationPolicyKey.class, 2, new IndexRecord.ValidationPolicyKey.Serializer())
               .serializer(IndexRecord.SyncdTillKey.class, 3, new IndexRecord.SyncdTillKey.Serializer())
               .serializer(IndexRecord.GroupPropertyKey.class, 4, new IndexRecord.GroupPropertyKey.Serializer())
               .serializer(IndexRecord.SchemaInfoKey.class, 5, new IndexRecord.SchemaInfoKey.Serializer())
               .serializer(IndexRecord.EncodingInfoIndex.class, 6, new IndexRecord.EncodingInfoIndex.Serializer())
               .serializer(IndexRecord.EncodingIdIndex.class, 7, new IndexRecord.EncodingIdIndex.Serializer());
    }

    /**
     * Serializes the given {@link IndexRecord.IndexKey} to a {@link ByteBuffer}.
     *
     * @param value The {@link IndexRecord.IndexKey} to serialize.
     * @return A base 64 encoding of wrapping an array that contains the serialization.
     */
    @SneakyThrows(IOException.class)
    public String toKeyString(IndexRecord.IndexKey value) {
        ByteArraySegment s = serialize(value);
        return value.getClass().getSimpleName() + "_" + Base64.getEncoder().encodeToString(s.getCopy());
    }
    
    /**
     * Deserializes the given base 64 encoded key string into a {@link IndexRecord.IndexKey} instance.
     *
     * @param string string to deserialize into key.
     * @return A new {@link IndexRecord.IndexKey} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public IndexRecord.IndexKey fromString(String string) {
        String[] tokens = string.split("_");
        
        byte[] buffer = Base64.getDecoder().decode(tokens[1]);
        return deserialize(new ByteArraySegment(buffer));
    }
}
