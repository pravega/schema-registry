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

import java.nio.ByteBuffer;
import java.util.Map;

class MultiplexedSerializer<T> implements Serializer<T> {
    private final Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializers;

    MultiplexedSerializer(Map<Class<? extends T>, AbstractPravegaSerializer<T>> serializers) {
        this.serializers = serializers;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public ByteBuffer serialize(T obj) {
        Class<? extends T> tClass = (Class<? extends T>) obj.getClass();
        AbstractPravegaSerializer<T> serializer = serializers.get(tClass);
        return serializer.serialize(obj);
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        throw new IllegalStateException();
    }
}