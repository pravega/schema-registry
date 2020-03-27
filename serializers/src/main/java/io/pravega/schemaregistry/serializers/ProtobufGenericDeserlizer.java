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
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import lombok.SneakyThrows;
import org.apache.commons.lang3.SerializationException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class ProtobufGenericDeserlizer extends AbstractPravegaDeserializer<DynamicMessage> {
    private final LoadingCache<SchemaInfo, Descriptors.Descriptor> knownSchemas;

    ProtobufGenericDeserlizer(String groupId, SchemaRegistryClient client, @Nullable ProtobufSchema<DynamicMessage> schema,
                              SerializerConfig.Decoder decoder, EncodingCache encodingCache) {
        super(groupId, client, schema, false, decoder, encodingCache);
        this.knownSchemas = CacheBuilder.newBuilder().build(new CacheLoader<SchemaInfo, Descriptors.Descriptor>() {
            @Override
            public Descriptors.Descriptor load(SchemaInfo schemaToUse) throws Exception {
                DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(schemaToUse.getSchemaData());
                int count = descriptorSet.getFileCount();
                int nameStart = schemaToUse.getName().lastIndexOf(".");
                String name = schemaToUse.getName().substring(nameStart + 1);
                String pckg = nameStart < 0 ? "" : schemaToUse.getName().substring(0, nameStart);
                DescriptorProtos.FileDescriptorProto mainDescriptor = descriptorSet.getFileList().stream()
                        .filter(x -> pckg.startsWith(x.getPackage()) &&
                                x.getMessageTypeList().stream().anyMatch(y -> y.getName().equals(name)))
                        .findAny().orElseThrow(IllegalArgumentException::new);
                
                Descriptors.FileDescriptor[] dependencyArray = new Descriptors.FileDescriptor[count];
                for (int i = 0; i < count; i++) {
                    Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
                            descriptorSet.getFile(i),
                            new Descriptors.FileDescriptor[]{});
                    dependencyArray[i] = fd;
                }

                Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(mainDescriptor, dependencyArray);

                return fd.getMessageTypes().stream().filter(x -> x.getName().equals(name))
                                                       .findAny().orElseThrow(() -> new SerializationException(String.format("schema for %s not found", schemaToUse.getName())));
            }
        });

    }

    @SneakyThrows
    @Override
    protected DynamicMessage deserialize(ByteBuffer buffer, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) {
        Preconditions.checkArgument(writerSchemaInfo != null || readerSchemaInfo != null);
        
        SchemaInfo schemaToUse = readerSchemaInfo == null ? writerSchemaInfo : readerSchemaInfo;
        Descriptors.Descriptor messageType = knownSchemas.get(schemaToUse);

        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return DynamicMessage.parseFrom(messageType, array);
    }
}
