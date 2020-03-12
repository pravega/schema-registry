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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import lombok.SneakyThrows;
import org.apache.commons.lang3.SerializationException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

public class ProtobufGenericDeserlizer extends AbstractPravegaDeserializer<DynamicMessage> {
    private final LoadingCache<SchemaInfo, Descriptors.Descriptor> cache;

    ProtobufGenericDeserlizer(String groupId, SchemaRegistryClient client, @Nullable ProtobufSchema<DynamicMessage> schema,
                              BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress, EncodingCache encodingCache) {
        super(groupId, client, schema, false, uncompress, encodingCache);
        this.cache = CacheBuilder.newBuilder().build(new CacheLoader<SchemaInfo, Descriptors.Descriptor>() {
            @Override
            public Descriptors.Descriptor load(SchemaInfo schemaToUse) throws Exception {
                DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaToUse.getSchemaData());
                int count = descriptorSet.getFileCount();
                DescriptorProtos.FileDescriptorProto mainDescriptor = descriptorSet
                        .getFileList().stream().filter(x -> {
                            return x.getMessageTypeList().stream().anyMatch(y -> y.getName().equals(schemaToUse.getName()));
                                }).findAny().orElseThrow(IllegalArgumentException::new);
                
                Descriptors.FileDescriptor[] dependencyArray = new Descriptors.FileDescriptor[count];
                for (int i = 0; i < count; i++) {
                    Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
                            descriptorSet.getFile(i),
                            new Descriptors.FileDescriptor[]{});
                    dependencyArray[i] = fd;
                }

                Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(mainDescriptor, dependencyArray);

                return fd.getMessageTypes().stream().filter(x -> x.getName().equals(schemaToUse.getName()))
                                                       .findAny().orElseThrow(() -> new SerializationException(String.format("schema for %s not found", schemaToUse.getName())));
            }
        });

    }

    @SneakyThrows
    @Override
    protected DynamicMessage deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkArgument(writerSchemaInfo != null || readerSchemaInfo != null);
        
        SchemaInfo schemaToUse = readerSchemaInfo == null ? writerSchemaInfo : readerSchemaInfo;
        Descriptors.Descriptor messageType = cache.get(schemaToUse);

        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return DynamicMessage.parseFrom(messageType, array);
    }
}
