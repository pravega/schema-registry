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
import io.pravega.schemaregistry.compression.Compressor;
import io.pravega.schemaregistry.schemas.SchemaData;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.schemaregistry.contract.SchemaRegistryContract.*;

public abstract class AbstractPravegaSerializer<T> {
    private static final byte PROTOCOL = 0x0;
    
    private final String scope;
    private final String groupId;
    private final String subGroupId;
    private final AtomicReference<SchemaInfo> schema;
    private final AtomicReference<VersionInfo> version;
    
    private final SchemaRegistryClient client;
    
    private final SerializerConfig config;
    
    @Getter
    private final Compressor compressor;
    @Getter
    private final boolean registerSchema;
    
    private boolean encodeHeader;
    
    protected AbstractPravegaSerializer(String scope,
                                        String groupId,
                                        String subGroupId,
                                        SchemaRegistryClient client,
                                        @Nullable SchemaData<T> schema,
                                        SerializerConfig config,
                                        Compressor compressor) {
        this.scope = scope;
        this.groupId = groupId;
        this.subGroupId = subGroupId;
        this.client = client;
        this.schema = new AtomicReference<>();
        this.version = new AtomicReference<>();
        if (schema != null) {
            this.schema.set(schema.getSchemaInfo());
        }
        this.config = config;
        this.compressor = compressor;
        this.registerSchema = config.isAutomaticallyRegisterSchema();
        this.encodeHeader = config.isEncodeHeader();
        initialize();
    }
    
    private void initialize() {
        if (schema.get() == null) {
            SchemaWithVersion latestSchema = client.getLatestSchema(scope, groupId, subGroupId);
            this.schema.compareAndSet(null, latestSchema.getSchema());
            this.version.compareAndSet(null, latestSchema.getVersion());
        } else {
            if (registerSchema) {
                // register schema 
                this.version.compareAndSet(null, 
                        client.addSchemaIfAbsent(scope, groupId, schema.get(), config.getValidationRules()));
            } else {
                // get already registered schema version. If schema is not registered, this will throw an exception. 
                this.version.compareAndSet(null, client.getSchemaVersion(scope, groupId, schema.get()));
            }
        }
    }

    @SneakyThrows
    ByteBuffer serialize(T obj) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        if (this.encodeHeader) {
            Preconditions.checkNotNull(schema);
            EncodingId encodingId = client.getEncodingId(scope, groupId, version.get(), compressor.getCompressionType());

            outputStream.write(PROTOCOL);
            outputStream.write(encodingId.getId());
        }
        
        // if schema is not null, pass the schema to the serializer implementation
        if (schema.get() != null) {
            serialize(obj, schema.get(), outputStream);
        } else {
            serialize(obj, null, outputStream);
        }

        outputStream.flush();

        return compressor.compress(ByteBuffer.wrap(outputStream.toByteArray()));
    }
    
    protected abstract void serialize(T var, SchemaInfo schema, OutputStream outputStream);
}
