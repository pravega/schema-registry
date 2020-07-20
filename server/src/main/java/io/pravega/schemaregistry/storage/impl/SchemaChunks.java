/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
public class SchemaChunks {
    private static final int MAX_CHUNK_SIZE = 921600; // setting max chunk size to 900 kb as KVT allows for key + value to be 1mb

    private final SchemaInfo schemaInfo;
    private final List<ByteArraySegment> chunks;

    private SchemaChunks(SchemaInfo schemaInfo, List<ByteArraySegment> chunks) {
        this.schemaInfo = schemaInfo;
        this.chunks = chunks;
    }

    public static SchemaChunks chunk(SchemaInfo schemaInfo) {
        int numberOfChunks = schemaInfo.getSchemaData().remaining() / MAX_CHUNK_SIZE;
        int firstChunkSize = numberOfChunks > 0 ? MAX_CHUNK_SIZE : schemaInfo.getSchemaData().remaining();

        ByteBuffer firstChunk = ByteBuffer.wrap(schemaInfo.getSchemaData().array(), schemaInfo.getSchemaData().position(), firstChunkSize);
        SchemaInfo chunkedSchemaInfo = new SchemaInfo(schemaInfo.getType(), schemaInfo.getSerializationFormat(),
                firstChunk, schemaInfo.getProperties());
        List<ByteArraySegment> chunks = new ArrayList<>();

        // break schema binary into smaller chunks.
        for (int i = 1; i <= numberOfChunks; i++) {
            int offset = i * MAX_CHUNK_SIZE;
            int length = schemaInfo.getSchemaData().remaining() > offset + MAX_CHUNK_SIZE ? MAX_CHUNK_SIZE :
                    schemaInfo.getSchemaData().remaining() - offset;
            ByteArraySegment chunk = new ByteArraySegment(schemaInfo.getSchemaData().array(), offset, length);
            chunks.add(chunk);
        }
        return new SchemaChunks(chunkedSchemaInfo, chunks);
    }

    public static SchemaInfo combine(SchemaInfo schemaInfo, List<ByteArraySegment> chunks) {
        // collect all chunks and create a new byte buffer as concat of all chunks
        ByteBuffer schemaData = schemaInfo.getSchemaData();

        ByteBuffer combined = ByteBuffer.allocate(schemaData.remaining() +
                chunks.stream().mapToInt(ByteArraySegment::getLength).reduce(0, Integer::sum));

        combined.put(schemaData.array(), schemaData.arrayOffset() + schemaData.position(), schemaData.remaining());
        chunks.forEach(x -> combined.put(x.array(), x.arrayOffset(), x.getLength()));
        return new SchemaInfo(schemaInfo.getType(), schemaInfo.getSerializationFormat(),
                combined, schemaInfo.getProperties());
    }
}
