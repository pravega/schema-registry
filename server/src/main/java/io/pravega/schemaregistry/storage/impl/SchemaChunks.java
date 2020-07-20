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

/**
 * Utility class for chunking a schema info into multiple chunks if the serialized schema info may exceed the limit of 
 * KVT key+value size limit of 1 Mb. 
 * We set the chunking limit as 900 Kb so that we have about 100 Kb for additional schema info fields and serialization overheads. 
 * The schema binary is chunked into chunks of 900 Kb each. 
 * So if there is a schema of 8 Mb in size, the number of chunks will be 10 chunks overall.
 * The first chunk is always included in the schema info object while remaining chunks are presented as list of byte array segments.
 */
@Data
public class SchemaChunks {
    private static final int MAX_CHUNK_SIZE = 921600; // setting max chunk size to 900 kb as KVT allows for key + value to be 1mb

    private final SchemaInfo schemaInfo;
    private final List<ByteArraySegment> chunks;

    private SchemaChunks(SchemaInfo schemaInfo, List<ByteArraySegment> chunks) {
        this.schemaInfo = schemaInfo;
        this.chunks = chunks;
    }

    /**
     * Chunks a schema binary from {@link SchemaInfo#getSchemaData()} into multiple smaller chunks by dividing the array
     * into smaller arrays of maximum 900 kb size. This returns a schema chunk object which can be assembled back into
     * original schema info object. 
     * The schema chunk contains a schema info, which has all the fields and a chunked schema data that has the first chunk.
     * The remaining chunks are available as list of byte array segments.  
     * @param schemaInfo Schema info to chunk
     * @return SchemaChunks object
     */
    public static SchemaChunks chunk(SchemaInfo schemaInfo) {
        int numberOfChunks = schemaInfo.getSchemaData().remaining() % MAX_CHUNK_SIZE == 0 ?
                schemaInfo.getSchemaData().remaining() / MAX_CHUNK_SIZE - 1 :
                schemaInfo.getSchemaData().remaining() / MAX_CHUNK_SIZE;
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

    /**
     * Assembles list of chunks and schema info into a new combined SchemaInfo object.
     * 
     * @param schemaInfo schemainfo
     * @param chunks chunks
     * @return SchemaInfo which created from taking the fields from schemaInfo and schema data is a combination of all 
     * binary data in schemainfo and all the chunks assembled into a single byte array.
     */
    public static SchemaInfo assemble(SchemaInfo schemaInfo, List<ByteArraySegment> chunks) {
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
