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

import io.pravega.common.util.ByteArraySegment;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ChunkUtilTest {
    @Test
    public void chunkTest() {
        List<ByteArraySegment> sc = ChunkUtil.chunk(ByteBuffer.wrap(new byte[100]), 900 * 1024);
        assertEquals(sc.size(), 1);
        assertEquals(sc.get(0).getLength(), 100);

        sc = ChunkUtil.chunk(ByteBuffer.wrap(new byte[0]), 900 * 1024);
        assertEquals(sc.size(), 1);
        assertEquals(sc.get(0).getLength(), 0);

        // schema data = 900 kb. it should still result in a single chunk
        sc = ChunkUtil.chunk(ByteBuffer.wrap(new byte[900 * 1024]), 900 * 1024);
        assertEquals(sc.size(), 1);
        assertEquals(sc.get(0).getLength(), 900 * 1024);

        // schema data 1mb > 900 kb. it should result in 2 chunks. 
        sc = ChunkUtil.chunk(ByteBuffer.wrap(new byte[1024 * 1024]), 900 * 1024);
        assertEquals(sc.size(), 2);
        assertEquals(sc.get(0).getLength(), 900 * 1024);
        assertEquals(sc.get(1).getLength(), 124 * 1024);

        // schema data 2700kb > 900 kb. it should result in 3 chunks. 
        sc = ChunkUtil.chunk(ByteBuffer.wrap(new byte[3 * 900 * 1024]), 900 * 1024);
        assertEquals(sc.size(), 3);
        assertEquals(sc.get(0).getLength(), 900 * 1024);
        assertEquals(sc.get(1).getLength(), 900 * 1024);
        assertEquals(sc.get(2).getLength(), 900 * 1024);
    }
}
