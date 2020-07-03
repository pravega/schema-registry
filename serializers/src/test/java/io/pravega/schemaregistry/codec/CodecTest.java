/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.codec;

import com.google.common.base.Charsets;
import io.pravega.schemaregistry.serializers.Codecs;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CodecTest {
    @Test
    public void testCodec() throws IOException {
        byte[] testStringBytes = "this is a test string".getBytes(Charsets.UTF_8);
        Codec snappy = Codecs.SnappyCompressor.getCodec();
        assertEquals(snappy.getCodecType(), Codecs.SnappyCompressor.getMimeType());
        ByteBuffer encoded = snappy.encode(ByteBuffer.wrap(testStringBytes));
        assertFalse(Arrays.equals(encoded.array(), testStringBytes));
        ByteBuffer decoded = snappy.decode(encoded);
        assertTrue(Arrays.equals(decoded.array(), testStringBytes));
        
        Codec gzip = Codecs.GzipCompressor.getCodec();
        assertEquals(gzip.getCodecType(), Codecs.GzipCompressor.getMimeType());
        encoded = gzip.encode(ByteBuffer.wrap(testStringBytes));
        assertFalse(Arrays.equals(encoded.array(), testStringBytes));
        decoded = gzip.decode(encoded);
        assertTrue(Arrays.equals(decoded.array(), testStringBytes));

        Codec none = Codecs.None.getCodec();
        assertEquals(none.getCodecType(), Codecs.None.getMimeType());
        encoded = none.encode(ByteBuffer.wrap(testStringBytes));
        assertTrue(Arrays.equals(encoded.array(), testStringBytes));
        decoded = none.decode(encoded);
        assertTrue(Arrays.equals(decoded.array(), testStringBytes));
    }
}
