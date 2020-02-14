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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.Compatibility;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public class PravegaSerDe<T> implements Serializer<T> {
    private final AbstractPravegaSerializer<T> abstractPravegaSerializer;
    private final AbstractPravegaDeserializer<T> abstractPravegaDeserializer;
    private final SchemaRegistryClient registryClient;

    protected PravegaSerDe(AbstractPravegaSerializer<T> abstractPravegaSerializer,
                           AbstractPravegaDeserializer<T> abstractPravegaDeserializer, SchemaRegistryClient registryClient) {
        this.abstractPravegaSerializer = abstractPravegaSerializer;
        this.abstractPravegaDeserializer = abstractPravegaDeserializer;
        this.registryClient = registryClient;
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

    public void addGroup(String scope, String groupId, SchemaRegistryContract.SchemaType schemaType, Compatibility compatibility,
                          boolean allowSubgroups, boolean encodeHeader) {
        registryClient.addGroup(scope, groupId, schemaType, compatibility, allowSubgroups, encodeHeader);
    }

}