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
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
abstract class AbstractPravegaDeserializer<T> implements Serializer<T> {
    private static final byte PROTOCOL = 0x0;
    private static final int HEADER_SIZE = 1 + Integer.BYTES;

    private final String groupId;
    private final SchemaRegistryClient client;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    // If headers are not encoded, then this will be the latest schema from the registry
    private final SchemaInfo schemaInfo;
    private final AtomicBoolean encodeHeader;
    private final SerializerConfig.Decoder decoder;
    private final boolean skipHeaders;
    private final EncodingCache encodingCache;
    
    protected AbstractPravegaDeserializer(String groupId,
                                          SchemaRegistryClient client,
                                          @Nullable SchemaContainer<T> schema,
                                          boolean skipHeaders,
                                          SerializerConfig.Decoder decoder,
                                          EncodingCache encodingCache) {
        this.groupId = groupId;
        this.client = client;
        this.encodingCache = encodingCache;
        this.schemaInfo = schema == null ? null : schema.getSchemaInfo();
        this.encodeHeader = new AtomicBoolean();
        this.skipHeaders = skipHeaders;
        this.decoder = decoder;
            
        initialize();
    }

    @Synchronized
    private void initialize() {
        GroupProperties groupProperties = client.getGroupProperties(groupId);

        Map<String, String> properties = groupProperties.getProperties();
        boolean toEncodeHeader = !properties.containsKey(SerializerFactory.ENCODE) ||
                Boolean.parseBoolean(properties.get(SerializerFactory.ENCODE));
        this.encodeHeader.set(toEncodeHeader);

        if (schemaInfo != null) {
            log.info("Validate caller supplied schema.");
            if (!client.canReadUsing(groupId, schemaInfo)) {
                throw new IllegalArgumentException("Cannot read using schema" + schemaInfo.getType());
            }
        } else {
            if (!this.encodeHeader.get()) {
                log.warn("No reader schema is supplied and stream does not have encoding headers.");
            }
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
            CodecType codecType = CodecType.None;
            if (skipHeaders) {
                int currentPos = data.position();
                data.position(currentPos + HEADER_SIZE);
            } else {
                byte protocol = data.get();
                EncodingId encodingId = new EncodingId(data.getInt());
                EncodingInfo encodingInfo = encodingCache.getGroupEncodingInfo(encodingId);
                codecType = encodingInfo.getCodec();
                writerSchema = encodingInfo.getSchemaInfo();
            }
            
            ByteBuffer uncompressed = decoder.decode(codecType, data);
            byte[] array = new byte[uncompressed.remaining()];
            uncompressed.get(array);

            InputStream inputStream = new ByteArrayInputStream(array);
            if (schemaInfo == null) { // deserialize into writer schema
                // pass writer schema for schema to be read into
                return deserialize(inputStream, writerSchema, writerSchema);
            } else {
                // pass reader schema for schema on read to the underlying implementation
                return deserialize(inputStream, writerSchema, schemaInfo);
            }
        } else {
            // pass reader schema for schema on read to the underlying implementation
            byte[] array = new byte[data.remaining()];
            data.get(array);
            InputStream inputStream = new ByteArrayInputStream(array);

            return deserialize(inputStream, null, schemaInfo);
        }
    }
    
    protected abstract T deserialize(InputStream inputStream, SchemaInfo writerSchema, SchemaInfo readerSchema);
    
    boolean isEncodeHeader() {
        return encodeHeader.get();
    }
}
