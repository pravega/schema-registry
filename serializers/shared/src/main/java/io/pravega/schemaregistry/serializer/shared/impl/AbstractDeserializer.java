/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.impl;

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.serializer.shared.schemas.Schema;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

@Slf4j
public abstract class AbstractDeserializer<T> extends ClosableDeserializer<T> {
    private static final int HEADER_SIZE = 1 + Integer.BYTES;

    private final String groupId;
    private final SchemaRegistryClient client;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    // If headers are not encoded, then this will be the latest schema from the registry
    private final SchemaInfo schemaInfo;
    private final boolean encodeHeader;
    private final SerializerConfig.Decoders decoders;
    private final boolean skipHeaders;
    private final EncodingCache encodingCache;
    private final boolean canCloseClient;

    protected AbstractDeserializer(String groupId,
                                   SchemaRegistryClient client,
                                   @Nullable Schema<T> schema,
                                   boolean skipHeaders,
                                   SerializerConfig.Decoders decoders,
                                   EncodingCache encodingCache,
                                   boolean encodeHeader, boolean canCloseClient) {
        this.canCloseClient = canCloseClient;
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(encodingCache);
        this.groupId = groupId;
        this.client = client;
        this.encodingCache = encodingCache;
        this.schemaInfo = schema == null ? null : schema.getSchemaInfo();
        this.encodeHeader = encodeHeader;
        this.skipHeaders = skipHeaders;
        this.decoders = decoders;
            
        initialize();
    }

    private void initialize() {
        if (schemaInfo != null) {
            log.info("Validate caller supplied schema.");
            if (!client.canReadUsing(groupId, schemaInfo)) {
                throw new IllegalArgumentException("Cannot read using schema" + schemaInfo.getType() + " as it is considered incompatible with current policy.");
            }
        } else {
            if (!this.encodeHeader) {
                log.warn("No reader schema is supplied and stream does not have encoding headers.");
            }
        }
    }
    
    @SneakyThrows(IOException.class)
    @Override
    public T deserialize(ByteBuffer data) {
        int start = data.hasArray() ? data.arrayOffset() + data.position() : data.position();
        ByteArrayInputStream inputStream;
        SchemaInfo writerSchema;
        SchemaInfo readerSchema;
        if (this.encodeHeader) {
            ByteBuffer decoded;
            if (skipHeaders) {
                data.position(start + HEADER_SIZE);
                decoded = data;
                writerSchema = null;
            } else {
                byte protocol = data.get();
                EncodingId encodingId = new EncodingId(data.getInt());
                EncodingInfo encodingInfo = encodingCache.getGroupEncodingInfo(encodingId);
                writerSchema = encodingInfo.getSchemaInfo();
                decoded = decoders.decode(encodingInfo.getCodecType(), data);
            }

            inputStream = new ByteArrayInputStream(decoded.array(), 
                    decoded.arrayOffset() + decoded.position(), decoded.remaining());
            // pass writer schema for schema to be read into
            readerSchema = schemaInfo == null ? writerSchema : schemaInfo;
        } else {
            byte[] b;
            if (data.hasArray()) {
                b = data.array();
            } else {
                b = new byte[data.remaining()];
                data.get(b);
            }
            writerSchema = null;
            readerSchema = schemaInfo;
            // pass reader schema for schema on read to the underlying implementation
            inputStream = new ByteArrayInputStream(b, start, data.remaining());
        }

        return deserialize(inputStream, writerSchema, readerSchema);
    }
    
    public abstract T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema) throws IOException;
    
    protected boolean isEncodeHeader() {
        return encodeHeader;
    }

    @Override
    public void close() throws Exception {
        if (canCloseClient) {
            client.close();
        }
    }
}
