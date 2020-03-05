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

import io.pravega.common.util.BitConverter;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.schemas.SchemaData;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractPravegaDeserializer<T> implements PravegaDeserializer<T> {
    private static final byte PROTOCOL = 0x0;
    private static final int HEADER_SIZE = 1 + Integer.BYTES;

    private final String groupId;
    private final SchemaRegistryClient client;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    private final AtomicReference<SchemaInfo> schemaInfo;
    private final AtomicBoolean encodeHeader;
    private final Map<CompressionType, Compressor> compressors;
    private final boolean skipHeaders;
    private final EncodingCache encodingCache;
    
    protected AbstractPravegaDeserializer(String groupId,
                                          SchemaRegistryClient client,
                                          @Nullable SchemaData<T> schema,
                                          boolean skipHeaders,
                                          Map<CompressionType, Compressor> compressors, EncodingCache encodingCache) {
        this.groupId = groupId;
        this.client = client;
        this.encodingCache = encodingCache;
        this.schemaInfo = new AtomicReference<>();
        if (schema != null) {
            schemaInfo.set(schema.getSchemaInfo());
        }
        this.encodeHeader = new AtomicBoolean();
        this.skipHeaders = skipHeaders;
        this.compressors = compressors;
            
        initialize();
    }

    @Synchronized
    private void initialize() {
        GroupProperties groupProperties = client.getGroupProperties(groupId);
        SchemaValidationRules schemaValidationRules = groupProperties.getSchemaValidationRules();

        this.encodeHeader.set(groupProperties.isEnableEncoding());

        if (schemaInfo.get() != null) {
            client.validateSchema(groupId, schemaInfo.get(), schemaValidationRules);
        }
    }
    
    @Override
    public T deserialize(ByteBuffer data) {
        if (this.encodeHeader.get()) {
            SchemaInfo writerSchema = null;
            CompressionType compressionType = CompressionType.of(CompressionType.Type.None);
            if (skipHeaders) {
                int currentPos = data.position();
                data.position(currentPos + HEADER_SIZE);
            } else {
                byte[] bytes = new byte[Integer.BYTES];
                data.get(bytes, 1, Integer.BYTES);
                EncodingId encodingId = new EncodingId(BitConverter.readInt(bytes, 0));
                EncodingInfo encodingInfo = encodingCache.getEncodingInfo(encodingId);
                compressionType = encodingInfo.getCompression();
                writerSchema = encodingInfo.getSchemaInfo();
            }
            
            ByteBuffer uncompressed = compressors.get(compressionType).uncompress(data);
            
            if (schemaInfo.get() == null) { // deserialize into writer schema
                // pass writer schema for schema to be read into
                return deserialize(uncompressed, writerSchema, writerSchema);
            } else {
                // pass reader schema for schema on read to the underlying implementation
                return deserialize(uncompressed, writerSchema, schemaInfo.get());
            }
        } else {
            // pass reader schema for schema on read to the underlying implementation
            return deserialize(data, null, schemaInfo.get());
        }
    }

    
    protected abstract T deserialize(ByteBuffer buffer, SchemaInfo writerSchema, SchemaInfo readerSchema);
}
