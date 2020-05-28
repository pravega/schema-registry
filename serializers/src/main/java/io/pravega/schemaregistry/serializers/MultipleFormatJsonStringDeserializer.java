/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.util.Map;

class MultipleFormatJsonStringDeserializer extends AbstractPravegaDeserializer<String> {
    private final Map<SerializationFormat, AbstractPravegaDeserializer> genericDeserializers;
    private final ObjectMapper objectMapper = new ObjectMapper();

    MultipleFormatJsonStringDeserializer(String groupId, SchemaRegistryClient client,
                                         Map<SerializationFormat, AbstractPravegaDeserializer> genericDeserializers,
                                         SerializerConfig.Decoder decoder,
                                         EncodingCache encodingCache) {
        super(groupId, client, null, false, decoder, encodingCache);
        this.genericDeserializers = genericDeserializers;
    }

    @Override
    protected String deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) {
        Preconditions.checkNotNull(writerSchema);
        return toJsonString(genericDeserializers.get(writerSchema.getSerializationFormat()).deserialize(inputStream, writerSchema, readerSchema));
    }

    @SneakyThrows
    private String toJsonString(Object deserialize) {
        if (deserialize instanceof GenericRecord) {
            return deserialize.toString();
        } else if (deserialize instanceof DynamicMessage) {
            JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames().usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().build());
            return printer.print((DynamicMessage) deserialize);
        } else if (deserialize instanceof JSonGenericObject) {
            Map myobject = ((JSonGenericObject) deserialize).getObject();
            return objectMapper.writeValueAsString(myobject);
        } else {
            return deserialize.toString();
        }
    }
}