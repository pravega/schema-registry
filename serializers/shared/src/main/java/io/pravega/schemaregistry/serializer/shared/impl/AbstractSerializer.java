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
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.serializer.shared.codec.Encoder;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.serializer.shared.schemas.Schema;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractSerializer<T> extends BaseSerializer<T> {
    private static final byte PROTOCOL = 0x1;

    private final String groupId;
    
    private final SchemaInfo schemaInfo;
    private final AtomicReference<EncodingId> encodingId;
    private final boolean encodeHeader;
    private final SchemaRegistryClient client;
    @Getter
    private final Encoder encoder;
    private final boolean registerSchema;
    
    protected AbstractSerializer(String groupId,
                                 SchemaRegistryClient client,
                                 Schema<T> schema,
                                 Encoder encoder,
                                 boolean registerSchema, 
                                 boolean encodeHeader) {
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(encoder);
        Preconditions.checkNotNull(schema);
        Preconditions.checkArgument(encodeHeader || encoder.equals(Codecs.None.getCodec()), 
                "Cannot use encoder if encoder header is false.");
        this.groupId = groupId;
        this.client = client;
        this.schemaInfo = schema.getSchemaInfo();
        this.registerSchema = registerSchema;
        this.encodingId = new AtomicReference<>();
        this.encoder = encoder;
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
            encodingId.set(client.getEncodingId(groupId, version, encoder.getCodecType().getName()));
        }
    }
    
    @SneakyThrows(IOException.class)
    @Override
    public ByteBuffer serialize(T obj) {
        ByteBufferOutputStream outStream = new ByteBufferOutputStream();
        ByteBuffer byteBuffer;
        if (this.encodeHeader) {
            outStream.write(PROTOCOL);
            outStream.writeInt(encodingId.get().getId());
        }

        if (!this.encodeHeader || this.encoder.equals(Codecs.None.getCodec())) {
            // write serialized data to the output stream
            serialize(obj, schemaInfo, outStream);
        } else {
            // encode header is true and encoder is supplied, encode the data
            ByteBufferOutputStream serializedStream = new ByteBufferOutputStream();

            serialize(obj, schemaInfo, serializedStream);
            encoder.encode(ByteBuffer.wrap(serializedStream.getData().array()), outStream);
        }

        byteBuffer = ByteBuffer.wrap(outStream.getData().array(), 0, outStream.getData().getLength());

        return byteBuffer;
    }

    protected abstract void serialize(T var, SchemaInfo schema, OutputStream outputStream) throws IOException;
}
