/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializers.AbstractPravegaDeserializer;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;
import java.util.Map;

public class ProtobufGenericDeserlizer extends AbstractPravegaDeserializer<DynamicMessage> {
    public ProtobufGenericDeserlizer(String groupId, SchemaRegistryClient client,
                                     Map<CompressionType, Compressor> compressorMap, EncodingCache encodingCache) {
        super(groupId, client, null, false, compressorMap, encodingCache);
    }

    @SneakyThrows
    @Override
    protected DynamicMessage deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkArgument(writerSchemaInfo != null || readerSchemaInfo != null);
        
        SchemaInfo schemaToUse = readerSchemaInfo == null ? writerSchemaInfo : readerSchemaInfo;
        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaToUse.getSchemaData());
        int count = descriptorSet.getFileCount();
        DescriptorProtos.FileDescriptorProto mainDescriptor = descriptorSet.getFile(0);

        Descriptors.FileDescriptor[] dependencyArray = new Descriptors.FileDescriptor[count];
        for (int i = 0; i < count; i++) {
            Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
                    descriptorSet.getFile(i),
                    new Descriptors.FileDescriptor[]{});
            dependencyArray[i] = fd;
        }

        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(mainDescriptor, dependencyArray);

        Descriptors.Descriptor messageType = fd.getMessageTypes().stream().filter(x -> x.getName().equals(schemaToUse.getName()))
                                               .findAny().get();

        return DynamicMessage.parseFrom(messageType, buffer.array());
    }
}
