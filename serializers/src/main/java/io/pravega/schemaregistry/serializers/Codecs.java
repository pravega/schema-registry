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

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.pravega.schemaregistry.codec.Codec;
import io.pravega.schemaregistry.contract.data.CodecType;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for creating codecs for none, snappy or gzip. 
 */
public enum Codecs {
    None(Constants.NOOP, Constants.NOOP.getCodecType()),
    GzipCompressor(Constants.GZIP_CODEC, Constants.GZIP_CODEC.getCodecType()), 
    SnappyCompressor(Constants.SNAPPY_CODEC, Constants.SNAPPY_CODEC.getCodecType());

    @Getter
    private final Codec codec;
    @Getter
    private final CodecType codecType;
    
    Codecs(Codec codec, CodecType codecType) {
        this.codec = codec;  
        this.codecType = codecType;
    }

    private static class Noop implements Codec {
        private static final CodecType CODEC_TYPE_NONE = new CodecType(Constants.NONE);

        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_NONE;
        }

        @Override
        public ByteBuffer encode(ByteBuffer data) {
            return data;
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) {
            return data;
        }
    }

    private static class GZipCodec implements Codec {
        private static final CodecType CODEC_TYPE_GZIP = new CodecType(Constants.APPLICATION_X_GZIP);
        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_GZIP;
        }
        
        @Override
        public ByteBuffer encode(ByteBuffer data) throws IOException {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.remaining())) {
                GZIPOutputStream gzipOS = new GZIPOutputStream(bos);
                gzipOS.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
                gzipOS.close();
                return ByteBuffer.wrap(bos.toByteArray());
            }
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) throws IOException {
            ByteBufferBackedInputStream bis = new ByteBufferBackedInputStream(data);
            return ByteBuffer.wrap(IOUtils.toByteArray(new GZIPInputStream(bis)));
        }
    }

    private static class SnappyCodec implements Codec {
        private static final CodecType CODEC_TYPE_SNAPPY = new CodecType(Constants.APPLICATION_X_SNAPPY_FRAMED);
        @Override
        public CodecType getCodecType() {
            return CODEC_TYPE_SNAPPY;
        }
        
        @Override
        public ByteBuffer encode(ByteBuffer data) throws IOException {
            int capacity = Snappy.maxCompressedLength(data.remaining());
            ByteBuffer encoded = ByteBuffer.allocate(capacity);
            
            int size = Snappy.compress(data.array(), data.arrayOffset() + data.position(),
                    data.remaining(), encoded.array(), 0);
            encoded.limit(size);
            return encoded;
        }
        
        @Override
        public ByteBuffer decode(ByteBuffer data) throws IOException {
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
