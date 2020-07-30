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

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CacheTest {
    @Test
    public void testCache() throws ExecutionException {
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        String groupId = "groupId";
        EncodingId encodingId = new EncodingId(0);
        EncodingInfo encodingInfo = new EncodingInfo(new VersionInfo("name", 0, 0),
                new SchemaInfo("name", SerializationFormat.Avro, ByteBuffer.wrap(new byte[0]), ImmutableMap.of()), 
                Codecs.SnappyCompressor.getCodec().getCodecType());
        doAnswer(x -> encodingInfo).when(client).getEncodingInfo(eq(groupId), eq(encodingId));
        EncodingId encodingId2 = new EncodingId(1);
        EncodingInfo encodingInfo2 = new EncodingInfo(new VersionInfo("name", 0, 1),
                new SchemaInfo("name", SerializationFormat.Avro, ByteBuffer.wrap(new byte[0]), ImmutableMap.of()), 
                Codecs.SnappyCompressor.getCodec().getCodecType());
        doAnswer(x -> encodingInfo2).when(client).getEncodingInfo(eq(groupId), eq(encodingId2));
        EncodingId encodingId3 = new EncodingId(2);
        EncodingInfo encodingInfo3 = new EncodingInfo(new VersionInfo("name", 0, 2),
                new SchemaInfo("name", SerializationFormat.Avro, ByteBuffer.wrap(new byte[0]), ImmutableMap.of()), 
                Codecs.SnappyCompressor.getCodec().getCodecType());
        doAnswer(x -> encodingInfo3).when(client).getEncodingInfo(eq(groupId), eq(encodingId3));
        // create a cache with max size 2
        EncodingCache cache = new EncodingCache(groupId, client, 2);
        assertEquals(cache.getMapForCache().size(), 0);
        assertEquals(encodingInfo, cache.getGroupEncodingInfo(encodingId));
        assertEquals(cache.getMapForCache().size(), 1);
        assertEquals(encodingInfo2, cache.getGroupEncodingInfo(encodingId2));
        assertEquals(cache.getMapForCache().size(), 2);
        assertEquals(encodingInfo3, cache.getGroupEncodingInfo(encodingId3));
        assertEquals(cache.getMapForCache().size(), 2);
        assertTrue(cache.getMapForCache().containsKey(encodingId2));
        assertTrue(cache.getMapForCache().containsKey(encodingId3));
    }
}
