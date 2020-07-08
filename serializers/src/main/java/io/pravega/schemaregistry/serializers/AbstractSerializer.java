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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.schemas.Schema;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractSerializer<T> extends FailingSerializer<T> {
    private static final byte PROTOCOL = 0x0;

    private final String groupId;
    
    private final SchemaInfo schemaInfo;
    private final AtomicReference<EncodingId> encodingId;
    private final boolean encodeHeader;
    private final SchemaRegistryClient client;
    @Getter
    private final Codec codec;
    private final boolean registerSchema;
    
    protected AbstractSerializer(String groupId,
                                 SchemaRegistryClient client,
                                 Schema<T> schema,
                                 Codec codec,
                                 boolean registerSchema, 
                                 boolean encodeHeader) {
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(codec);
        Preconditions.checkNotNull(schema);
        
        this.groupId = groupId;
        this.client = client;
        this.schemaInfo = schema.getSchemaInfo();
        this.registerSchema = registerSchema;
        this.encodingId = new AtomicReference<>();
        this.codec = codec;
        this.encodeHeader = encodeHeader;
        initialize();
    }
    
    private void initialize() {
        VersionInfo version;
        if (registerSchema) {
            // register schema
            version = client.addSchema(groupId, schemaInfo);
        } else {
            // get already registered schema version. If schema is not registered, this will throw an exception. 
            version = client.getVersionForSchema(groupId, schemaInfo);
        }
        if (encodeHeader) {
            encodingId.set(client.getEncodingId(groupId, version, codec.getCodecType().getName()));
        }
    }
    
    @SneakyThrows(IOException.class)
    @Override
    public ByteBuffer serialize(T obj) {
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        
        // if schema is not null, pass the schema to the serializer implementation
        if (schemaInfo != null) {
            serialize(obj, schemaInfo, dataStream);
        } else {
            serialize(obj, null, dataStream);
        }

        dataStream.flush();

        byte[] serialized = dataStream.toByteArray();
        
        ByteBuffer byteBuffer;
        if (this.encodeHeader) {
            Preconditions.checkNotNull(schemaInfo);
            ByteBuffer encoded = codec.encode(ByteBuffer.wrap(serialized));
            int bufferSize = 5 + encoded.remaining();
            byteBuffer = ByteBuffer.allocate(bufferSize);
            byteBuffer.put(PROTOCOL);
            byteBuffer.putInt(encodingId.get().getId());
            byteBuffer.put(encoded);
            byteBuffer.rewind();
        } else {
            byteBuffer = ByteBuffer.wrap(serialized);
        }
        
        return byteBuffer;
    }

    protected abstract void serialize(T var, SchemaInfo schema, OutputStream outputStream) throws IOException;
}
