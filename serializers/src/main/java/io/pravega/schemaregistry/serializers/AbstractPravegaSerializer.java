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
import io.pravega.client.stream.Serializer;
import io.pravega.common.util.BitConverter;
import io.pravega.schemaregistry.cache.EncodingCache;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.schemas.SchemaContainer;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractPravegaSerializer<T> implements Serializer<T> {
    private static final byte PROTOCOL = 0x0;

    private final String groupId;
    
    private final SchemaInfo schemaInfo;
    private final AtomicReference<VersionInfo> version;
    private final AtomicBoolean encodeHeader;
    private final SchemaRegistryClient client;
    @Getter
    private final Compressor compressor;
    private final boolean registerSchema;
    private final EncodingCache encodingCache;

    protected AbstractPravegaSerializer(String groupId,
                                        SchemaRegistryClient client,
                                        SchemaContainer<T> schema,
                                        Compressor compressor, 
                                        boolean registerSchema,
                                        EncodingCache encodingCache) {
        Preconditions.checkNotNull(groupId);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(compressor);
        Preconditions.checkNotNull(encodingCache);
        Preconditions.checkNotNull(schema);
        
        this.groupId = groupId;
        this.client = client;
        this.schemaInfo = schema.getSchemaInfo();
        this.encodingCache = encodingCache;
        this.registerSchema = registerSchema;
        this.version = new AtomicReference<>();
        this.compressor = compressor;
        this.encodeHeader = new AtomicBoolean();
        initialize();
    }
    
    private void initialize() {
        GroupProperties groupProperties = client.getGroupProperties(groupId);

        Map<String, String> properties = groupProperties.getProperties();
        boolean toEncodeHeader = Boolean.parseBoolean(properties.get(SerializerFactory.ENCODE));
        encodeHeader.set(toEncodeHeader);
        if (registerSchema) {
            // register schema
            this.version.compareAndSet(null, 
                    client.addSchemaIfAbsent(groupId, schemaInfo));
        } else {
            // get already registered schema version. If schema is not registered, this will throw an exception. 
            this.version.compareAndSet(null, encodingCache.getVersionFromSchema(schemaInfo));
        }
    }
    
    @SneakyThrows
    @Override
    public ByteBuffer serialize(T obj) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        
        if (this.encodeHeader.get()) {
            Preconditions.checkNotNull(schemaInfo);
            EncodingId encodingId = encodingCache.getEncodingId(schemaInfo, compressor.getCompressionType());

            outputStream.write(PROTOCOL);
            BitConverter.writeInt(outputStream, encodingId.getId());
        }
        
        // if schema is not null, pass the schema to the serializer implementation
        if (schemaInfo != null) {
            serialize(obj, schemaInfo, dataStream);
        } else {
            serialize(obj, null, dataStream);
        }

        dataStream.flush();

        byte[] array = dataStream.toByteArray();

        ByteBuffer compressed = compressor.compress(ByteBuffer.wrap(array));
        array = new byte[compressed.remaining()];
        compressed.get(array);

        outputStream.write(array);
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    protected abstract void serialize(T var, SchemaInfo schema, OutputStream outputStream);

    @Override
    public T deserialize(ByteBuffer bytes) {
        throw new IllegalStateException();
    }
}
