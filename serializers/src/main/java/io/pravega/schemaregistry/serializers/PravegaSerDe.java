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
import io.pravega.client.stream.Serializer;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public class PravegaSerDe<T> implements Serializer<T> {
    private final PravegaSerializer<T> serializer;
    private final PravegaDeserializer<T> deserializer;

    PravegaSerDe(PravegaSerializer<T> serializer,
                 PravegaDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }
    
    @SneakyThrows
    @Override
    public ByteBuffer serialize(T obj) {
        Preconditions.checkNotNull(serializer);
        return serializer.serialize(obj);
    }

    @SneakyThrows
    @Override
    public T deserialize(ByteBuffer data) {
        Preconditions.checkNotNull(deserializer);
        return deserializer.deserialize(data);
    }
}