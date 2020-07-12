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
import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.common.NameUtil;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.ProtobufSchema;
import org.apache.commons.lang3.SerializationException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufGenericDeserlizer extends AbstractDeserializer<DynamicMessage> {
    private final ConcurrentHashMap<SchemaInfo, Descriptors.Descriptor> knownSchemas;

    ProtobufGenericDeserlizer(String groupId, SchemaRegistryClient client, @Nullable ProtobufSchema<DynamicMessage> schema,
                              SerializerConfig.Decoders decoder, EncodingCache encodingCache, boolean encodeHeader) {
        super(groupId, client, schema, false, decoder, encodingCache, encodeHeader);
        Preconditions.checkArgument(isEncodeHeader() || schema != null);
        knownSchemas = new ConcurrentHashMap<>();
    }

    @Override
    protected DynamicMessage deserialize(InputStream inputStream, SchemaInfo writerSchemaInfo, SchemaInfo readerSchemaInfo) throws IOException {
        Preconditions.checkArgument(writerSchemaInfo != null || readerSchemaInfo != null);

        SchemaInfo schemaToUse = readerSchemaInfo == null ? writerSchemaInfo : readerSchemaInfo;
        Descriptors.Descriptor messageType = knownSchemas.computeIfAbsent(schemaToUse, this::parseSchema);

        return DynamicMessage.parseFrom(messageType, inputStream);
    }

    private Descriptors.Descriptor parseSchema(SchemaInfo schemaToUse) {
        DescriptorProtos.FileDescriptorSet descriptorSet = ProtobufSchema.from(schemaToUse).getDescriptorProto();

        int count = descriptorSet.getFileCount();
        String[] tokens = NameUtil.extractNameAndQualifier(schemaToUse.getType());
        String name = tokens[0];
        String pckg = tokens[1];
        DescriptorProtos.FileDescriptorProto mainDescriptor = null;
        for (DescriptorProtos.FileDescriptorProto x : descriptorSet.getFileList()) {
            boolean packageMatch;
            if (x.getPackage() == null) {
                packageMatch = Strings.isNullOrEmpty(pckg);
            } else {
                packageMatch = x.getPackage().equals(pckg);
            }
            if (packageMatch && x.getMessageTypeList().stream().anyMatch(y -> y.getName().equals(name))) {
                mainDescriptor = x;
                break;
            }
        }
        if (mainDescriptor == null) {
            throw new IllegalArgumentException("FileDescriptorSet doesn't contain the schema for the object type.");
        }

        Descriptors.FileDescriptor[] dependencyArray = new Descriptors.FileDescriptor[count];
        Descriptors.FileDescriptor fd;
        try {
            for (int i = 0; i < count; i++) {
                fd = Descriptors.FileDescriptor.buildFrom(
                        descriptorSet.getFile(i),
                        new Descriptors.FileDescriptor[]{});
                dependencyArray[i] = fd;
            }

            fd = Descriptors.FileDescriptor.buildFrom(mainDescriptor, dependencyArray);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid protobuf schema.");
        }
        return fd.getMessageTypes().stream().filter(x -> x.getName().equals(name))
                 .findAny().orElseThrow(() -> new SerializationException(String.format("schema for %s not found", schemaToUse.getType())));
    }
}
