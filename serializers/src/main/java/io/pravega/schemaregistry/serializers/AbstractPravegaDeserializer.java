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
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

@Slf4j
abstract class AbstractPravegaDeserializer<T> implements Serializer<T> {
    private static final byte PROTOCOL = 0x0;
    private static final int HEADER_SIZE = 1 + Integer.BYTES;

    private final String groupId;
    private final SchemaRegistryClient client;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    private final AtomicReference<SchemaInfo> schemaInfo;
    private final AtomicBoolean encodeHeader;
    private final BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress;
    private final boolean skipHeaders;
    private final EncodingCache encodingCache;
    
    protected AbstractPravegaDeserializer(String groupId,
                                          SchemaRegistryClient client,
                                          @Nullable SchemaContainer<T> schema,
                                          boolean skipHeaders,
                                          BiFunction<CompressionType, ByteBuffer, ByteBuffer> uncompress, 
                                          EncodingCache encodingCache) {
        this.groupId = groupId;
        this.client = client;
        this.encodingCache = encodingCache;
        this.schemaInfo = new AtomicReference<>();
        if (schema != null) {
            schemaInfo.set(schema.getSchemaInfo());
        }
        this.encodeHeader = new AtomicBoolean();
        this.skipHeaders = skipHeaders;
        this.uncompress = uncompress;
            
        initialize();
    }

    @Synchronized
    private void initialize() {
        GroupProperties groupProperties = client.getGroupProperties(groupId);

        this.encodeHeader.set(groupProperties.isEnableEncoding());

        if (schemaInfo.get() != null) {
            log.info("Validate caller supplied schema.");
            if (!client.canRead(groupId, schemaInfo.get())) {
                throw new IllegalArgumentException("Cannot read using schema" + schemaInfo.get().getName());
            }
        } else if (!this.encodeHeader.get()) {
            log.info("Retrieving latest schema from the registry for reads.");
            schemaInfo.set(client.getLatestSchema(groupId, null).getSchema());
        } else {
            log.info("Read using writer schema.");
        }
    }
    
    @Override
    public ByteBuffer serialize(T obj) {
        throw new IllegalStateException();
    }
    
    @Override
    public T deserialize(ByteBuffer data) {
        if (this.encodeHeader.get()) {
            SchemaInfo writerSchema = null;
            CompressionType compressionType = CompressionType.None;
            if (skipHeaders) {
                int currentPos = data.position();
                data.position(currentPos + HEADER_SIZE);
            } else {
                byte protocol = data.get();
                EncodingId encodingId = new EncodingId(data.getInt());
                EncodingInfo encodingInfo = encodingCache.getEncodingInfo(encodingId);
                compressionType = encodingInfo.getCompression();
                writerSchema = encodingInfo.getSchemaInfo();
            }
            
            ByteBuffer uncompressed = uncompress.apply(compressionType, data);
            
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
