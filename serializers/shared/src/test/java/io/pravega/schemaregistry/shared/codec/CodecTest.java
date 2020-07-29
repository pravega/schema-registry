/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.shared.codec;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.*;

public class CodecTest {
    @Test
    public void testCodec() throws IOException {
        byte[] testStringBytes = "this is a test string".getBytes(Charsets.UTF_8);
        Codec snappy = Codecs.SnappyCompressor.getCodec();
        assertEquals(snappy.getCodecType(), Codecs.SnappyCompressor.getCodec().getCodecType());
        EnhancedByteArrayOutputStream byteArrayOutputStream = new EnhancedByteArrayOutputStream();
        snappy.encode(ByteBuffer.wrap(testStringBytes), byteArrayOutputStream);
        ByteBuffer encoded = ByteBuffer.wrap(byteArrayOutputStream.getData().array(), 0, byteArrayOutputStream.getData().getLength());
        assertNotEquals(encoded.remaining(), testStringBytes.length);
        ByteBuffer decoded = snappy.decode(encoded, ImmutableMap.of());
        assertTrue(Arrays.equals(decoded.array(), testStringBytes));

        byteArrayOutputStream = new EnhancedByteArrayOutputStream();
        Codec gzip = Codecs.GzipCompressor.getCodec();
        assertEquals(gzip.getCodecType(), Codecs.GzipCompressor.getCodec().getCodecType());
        gzip.encode(ByteBuffer.wrap(testStringBytes), byteArrayOutputStream);
        encoded = ByteBuffer.wrap(byteArrayOutputStream.getData().array(), 0, byteArrayOutputStream.getData().getLength());
        assertNotEquals(encoded.remaining(), testStringBytes.length);
        decoded = gzip.decode(encoded, ImmutableMap.of());
        assertTrue(Arrays.equals(decoded.array(), testStringBytes));
        
        byteArrayOutputStream = new EnhancedByteArrayOutputStream();
        Codec none = Codecs.None.getCodec();
        assertEquals(none.getCodecType(), Codecs.None.getCodec().getCodecType());
        none.encode(ByteBuffer.wrap(testStringBytes), byteArrayOutputStream);
        encoded = ByteBuffer.wrap(byteArrayOutputStream.getData().array(), 0, byteArrayOutputStream.getData().getLength());
        assertEquals(encoded.remaining(), testStringBytes.length);
        decoded = none.decode(encoded, ImmutableMap.of());
        
        byte[] decodedArray = new byte[decoded.remaining()];
        decoded.get(decodedArray);
        assertTrue(Arrays.equals(decodedArray, testStringBytes));
    }
}
