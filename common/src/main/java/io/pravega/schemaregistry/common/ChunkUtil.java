/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.common;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ByteArraySegment;
import lombok.Data;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for breaking a byte buffer into multiple byte buffer segments with length of each being being bounded by 
 * maximum size. 
 * Example: if we set the max chunking size as 900 KB. The buffer is chunked into chunks of 900 Kb each. 
 * So if there is a buffer of size 8 MB, the number of chunks will be 10 chunks overall with first 9 buffers being 900 KB each 
 * and last buffer of 92 KB.
 */
@Data
public class ChunkUtil {
    /**
     * Chunks a byte buffer into multiple smaller chunks by dividing the array into smaller array segments of maximum 900 KB size. 
     * This does not make a copy of the original buffer, instead creates ByteArraySegments on the given byte buffer 
     * @param byteBuffer Byte buffer to chunk
     * @param maxChunkSize maximum chunk size
     * @return List of ByteArraySegment
     */
    public static List<ByteArraySegment> chunk(@NonNull ByteBuffer byteBuffer, int maxChunkSize) {
        Preconditions.checkArgument(maxChunkSize > 0, "MaxChunkSize should be greater than 0");
        int remaining = byteBuffer.remaining();
        int numberOfChunks = remaining != 0 && remaining % maxChunkSize == 0 ? remaining / maxChunkSize : remaining / maxChunkSize + 1;
        List<ByteArraySegment> chunks = new ArrayList<>();

        // break buffer into smaller chunks.
        for (int i = 0; i < numberOfChunks; i++) {
            int offset = byteBuffer.arrayOffset() + byteBuffer.position() + i * maxChunkSize;
            int length = Math.min(remaining - offset, maxChunkSize);
            ByteArraySegment chunk = new ByteArraySegment(byteBuffer.array(), offset, length);
            chunks.add(chunk);
        }
        return chunks;
    }

    /**
     * Assembles list of chunks into a new combined byte buffer.
     * 
     * @param chunks chunks to assemble into a single buffer
     * @return Concats byte array segments into a single byte array.
     */
    public static ByteBuffer combine(List<ByteArraySegment> chunks) {
        // collect all chunks and create a new byte buffer as concat of all chunks
        ByteBuffer combined = ByteBuffer.allocate(chunks.stream().mapToInt(ByteArraySegment::getLength).reduce(0, Integer::sum));

        chunks.forEach(x -> combined.put(x.array(), x.arrayOffset(), x.getLength()));
        combined.rewind();
        return combined;
    }
}
