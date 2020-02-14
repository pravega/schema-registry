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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.schemas.SchemaData;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.CompressionType;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingId;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.EncodingInfo;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaInfo;
import static io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaWithVersion;

@Slf4j
public abstract class AbstractPravegaDeserializer<T> {
    private static final byte PROTOCOL = 0x0;
    
    private final String scope;
    private final String groupId;
    private final SchemaRegistryClient client;
    private final SerializerConfig config;
    // This can be null. If no schema is supplied, it means the intent is to deserialize into writer schema. 
    // Or the derived class will multiplex based on subgroup.  
    private final AtomicReference<SchemaInfo> schemaInfo;
    @Getter
    private final boolean skipHeaders;
    @Getter
    private final boolean automaticallyRegisterSchema;
    private boolean encodedHeader;
    private final BiFunction<ByteBuffer, CompressionType, ByteBuffer> uncompress;
    
    protected AbstractPravegaDeserializer(String scope,
                                          String groupId,
                                          SchemaRegistryClient client,
                                          @Nullable SchemaData<T> schema,
                                          SerializerConfig config,
                                          boolean skipHeaders,
                                          BiFunction<ByteBuffer, CompressionType, ByteBuffer> uncompress) {
        this.scope = scope;
        this.groupId = groupId;
        this.client = client;
        this.schemaInfo = new AtomicReference<>();
        if (schema != null){
            schemaInfo.set(schema.getSchemaInfo());
        }
        this.config = config;
        this.automaticallyRegisterSchema = config.isAutomaticallyRegisterSchema();
        this.skipHeaders = skipHeaders;
        this.uncompress = uncompress;
    }

    @Synchronized
    private void initialize() {
        if (schemaInfo.get() != null) {
            if (automaticallyRegisterSchema) {
                // register schema 
                client.addSchemaIfAbsent(scope, groupId, schemaInfo.get(), config.getValidationRules());
            } 
        } else {
            if (!config.isDeserializeIntoWriterSchema()) {
                // get the latest schema from the registry
                SchemaWithVersion latestSchema = client.getLatestSchema(scope, groupId, null);
                if (latestSchema == null) {
                    log.warn("No schema to read into");
                } else {
                    this.schemaInfo.compareAndSet(null, latestSchema.getSchema());
                }
            }
        }
    }
    
    public T deserialize(ByteBuffer data) {
        CompressionType compressionType = config.getCompressionType();
        if (encodedHeader) {
            SchemaInfo writerSchema = null;
            if (skipHeaders) {
                int currentPos = data.position();
                data.position(currentPos + 1 + Integer.BYTES);
            } else {
                byte[] bytes = new byte[Integer.BYTES];
                data.get(bytes, 1, Integer.BYTES);
                EncodingId encodingId = new EncodingId(BitConverter.readInt(bytes, 0));
                EncodingInfo encodingInfo = client.getEncodingInfo(scope, groupId, encodingId);
                compressionType = encodingInfo.getCompressionType();
                writerSchema = encodingInfo.getSchemaInfo();
            }
            ByteBuffer uncompressed = uncompress.apply(data, compressionType);
            if (schemaInfo.get() == null) { // deserialize into writer schema
                // pass writer schema for schema to be read into
                return deserialize(uncompressed, writerSchema, null);
            } else {
                // pass schema on read to the underlying implementation
                return deserialize(uncompressed, writerSchema, schemaInfo.get());
            }
        } else {
            // pass schema on read to the underlying implementation
            return deserialize(data, null, schemaInfo.get());
        }
    }

    
    protected abstract T deserialize(ByteBuffer buffer, SchemaInfo writerSchema, SchemaInfo readerSchema);
}
