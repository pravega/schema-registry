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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.shared.serializers.AbstractSerializer;
import org.apache.commons.lang3.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class MultipleFormatSerializer implements Serializer<WithSchema<Object>> {
    private final Function<SchemaInfo, AbstractSerializer<Object>> serializerFunction;

    private final ConcurrentHashMap<SchemaInfo, AbstractSerializer<Object>> serializersMap;

    MultipleFormatSerializer(Function<SchemaInfo, AbstractSerializer<Object>> serializerFunction) {
        this.serializerFunction = serializerFunction;
        this.serializersMap = new ConcurrentHashMap<>();
    }
    
    @Override
    public ByteBuffer serialize(WithSchema<Object> value) {
        AbstractSerializer<Object> serializer = serializersMap.computeIfAbsent(value.getSchema().getSchemaInfo(), 
                x -> serializerFunction.apply(value.getSchema().getSchemaInfo()));
        return serializer.serialize(value.getObject());
    }

    @Override
    public WithSchema<Object> deserialize(ByteBuffer serializedValue) {
        throw new NotImplementedException("Deserializer not implemented");
    }
}