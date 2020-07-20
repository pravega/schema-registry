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

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaChunkTest {
    @Test
    public void chunkTest() {
        SchemaInfo s = new SchemaInfo("", SerializationFormat.Avro, ByteBuffer.wrap(new byte[100]), ImmutableMap.of());
        SchemaChunks sc = SchemaChunks.chunk(s);
        assertEquals(sc.getSchemaInfo().getSchemaData().remaining(), 100);
        assertTrue(sc.getChunks().isEmpty());

        // schema data = 900 kb. it should still result in a single chunk
        s = new SchemaInfo("", SerializationFormat.Avro, ByteBuffer.wrap(new byte[900 * 1024]), ImmutableMap.of());
        sc = SchemaChunks.chunk(s);
        assertEquals(sc.getSchemaInfo().getSchemaData().remaining(), 900 * 1024);
        assertTrue(sc.getChunks().isEmpty());

        // schema data 1mb > 900 kb. it should result in 2 chunks. 
        s = new SchemaInfo("", SerializationFormat.Avro, ByteBuffer.wrap(new byte[1024 * 1024]), ImmutableMap.of());
        sc = SchemaChunks.chunk(s);
        assertEquals(sc.getSchemaInfo().getSchemaData().remaining(), 900 * 1024);
        assertEquals(sc.getChunks().size(), 1);
        assertEquals(sc.getChunks().get(0).getLength(), 124 * 1024);

        // schema data 2700kb > 900 kb. it should result in 3 chunks. 
        s = new SchemaInfo("", SerializationFormat.Avro, ByteBuffer.wrap(new byte[3 * 900 * 1024]), ImmutableMap.of());
        sc = SchemaChunks.chunk(s);
        assertEquals(sc.getSchemaInfo().getSchemaData().remaining(), 900 * 1024);
        assertEquals(sc.getChunks().size(), 2);
        assertEquals(sc.getChunks().get(0).getLength(), 900 * 1024);
        assertEquals(sc.getChunks().get(1).getLength(), 900 * 1024);
    }
}
