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

import io.pravega.client.stream.Serializer;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public class PravegaSerDeMultiplex<T> implements Serializer<T> {
    private final AbstractPravegaSerializer<T> abstractPravegaSerializer;
    private final AbstractPravegaDeserializer<T> abstractPravegaDeserializer;

    public PravegaSerDeMultiplex(AbstractPravegaSerializer<T> abstractPravegaSerializer,
                                 AbstractPravegaDeserializer<T> abstractPravegaDeserializer) {
        this.abstractPravegaSerializer = abstractPravegaSerializer;
        this.abstractPravegaDeserializer = abstractPravegaDeserializer;
    }

    @SneakyThrows
    @Override
    public ByteBuffer serialize(T obj) {
        return abstractPravegaSerializer.serialize(obj);
    }

    @SneakyThrows
    @Override
    public T deserialize(ByteBuffer data) {
        return abstractPravegaDeserializer.deserialize(data);
    }

}