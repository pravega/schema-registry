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

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.pravega.schemaregistry.contract.data.CodecType;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for creating codecs for none, snappy or gzip. 
 */
public enum Codecs {
    None(Constants.NOOP),
    GzipCompressor(Constants.GZIP_CODEC), 
    SnappyCompressor(Constants.SNAPPY_CODEC);

    @Getter
    private final Codec codec;
    
    Codecs(Codec codec) {
        this.codec = codec;  
    }

    private static class Noop implements Codec {
        private static final CodecType CODEC_TYPE_NONE = new CodecType(Constants.NONE);

        @Override
        public String getName() {
            return CODEC_TYPE_NONE.getName();
        }

        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_NONE;
        }

        @Override
        public void encode(ByteBuffer data, ByteArrayOutputStream bos) {
            bos.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
        }

        @Override
        public ByteBuffer decode(ByteBuffer data, Map<String, String> codecProperties) {
            return data;
        }
    }

    private static class GZipCodec implements Codec {
        private static final CodecType CODEC_TYPE_GZIP = new CodecType(Constants.APPLICATION_X_GZIP);
        @Override
        public String getName() {
            return CODEC_TYPE_GZIP.getName();
        }

        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_GZIP;
        }

        @Override
        public void encode(ByteBuffer data, ByteArrayOutputStream bos) throws IOException {
            try (GZIPOutputStream gzipOS = new GZIPOutputStream(bos)) {
                gzipOS.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
            }
        }

        @Override
        public ByteBuffer decode(ByteBuffer data, Map<String, String> codecProperties) throws IOException {
            InputStream bis = new ByteBufferBackedInputStream(data);
            return ByteBuffer.wrap(IOUtils.toByteArray(new GZIPInputStream(bis)));
        }
    }

    private static class SnappyCodec implements Codec {
        private static final CodecType CODEC_TYPE_SNAPPY = new CodecType(Constants.APPLICATION_X_SNAPPY_FRAMED);
        @Override
        public String getName() {
            return CODEC_TYPE_SNAPPY.getName();
        }

        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_SNAPPY;
        }

        @Override
        public void encode(ByteBuffer data, ByteArrayOutputStream bos) throws IOException {
            int capacity = Snappy.maxCompressedLength(data.remaining());
            byte[] encoded = new byte[capacity];
            
            int size = Snappy.compress(data.array(), data.arrayOffset() + data.position(),
                    data.remaining(), encoded, 0);
            bos.write(encoded, 0, size);
        }
        
        @Override
        public ByteBuffer decode(ByteBuffer data, Map<String, String> codecProperties) throws IOException {
            ByteBuffer decoded = ByteBuffer.allocate(Snappy.uncompressedLength(data.array(), data.arrayOffset() + data.position(),
                    data.remaining()));
            Snappy.uncompress(data.array(), data.arrayOffset() + data.position(), 
                    data.remaining(), decoded.array(), 0);
            return decoded;
        }
    }

    static class Constants {
        static final Noop NOOP = new Noop();
        static final GZipCodec GZIP_CODEC = new GZipCodec();
        static final SnappyCodec SNAPPY_CODEC = new SnappyCodec();
        static final String NONE = "";
        static final String APPLICATION_X_GZIP = "application/x-gzip";
        static final String APPLICATION_X_SNAPPY_FRAMED = "application/x-snappy-framed";
    }
}
