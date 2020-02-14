/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.schemas;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import lombok.Data;
import lombok.Getter;

import javax.annotation.Nullable;

@Data
public class ProtobufSchema<T extends Message> implements SchemaData<T> {
    @Getter
    private final Parser<T> parser;
    @Getter
    private final DescriptorProtos.DescriptorProto descriptorProto;
    private final SchemaRegistryContract.SchemaInfo schemaInfo;

    public ProtobufSchema(String name, @Nullable Parser<T> parser, DescriptorProtos.DescriptorProto descriptorProto) {
        this.parser = parser;
        this.descriptorProto = descriptorProto;
        this.schemaInfo = new SchemaRegistryContract.SchemaInfo(
                name,
                SchemaRegistryContract.SchemaType.Protobuf, getSchemaBytes(), ImmutableMap.of());
    }

    @Override
    public byte[] getSchemaBytes() {
        return descriptorProto.toByteArray();
    }

    @Override
    public SchemaRegistryContract.SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}

