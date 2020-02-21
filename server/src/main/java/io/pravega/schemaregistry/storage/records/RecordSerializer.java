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

public class RecordSerializer extends VersionedSerializer.MultiType<Record> {

    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(Record.SchemaRecord.class, 1, new Record.SchemaRecord.Serializer())
               .serializer(Record.EncodingRecord.class, 2, new Record.EncodingRecord.Serializer())
               .serializer(Record.ValidationRecord.class, 3, new Record.ValidationRecord.Serializer());
    }

    /**
     * Serializes the given {@link Record} to a {@link ByteBuffer}.
     *
     * @param value The {@link Record} to serialize.
     * @return A new {@link ByteBuffer} wrapping an array that contains the serialization.
     */
    @SneakyThrows(IOException.class)
    public ByteBuffer toByteBuffer(Record value) {
        ByteArraySegment s = serialize(value);
        return ByteBuffer.wrap(s.array(), s.arrayOffset(), s.getLength());
    }

    /**
     * Deserializes the given {@link ByteBuffer} into a {@link Record} instance.
     *
     * @param buffer {@link ByteBuffer} to deserialize.
     * @return A new {@link Record} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public Record fromByteBuffer(ByteBuffer buffer) {
        return deserialize(new ByteArraySegment(buffer));
    }
}
