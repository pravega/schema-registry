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

import io.pravega.schemaregistry.codec.Codec;
import lombok.Getter;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for creating codecs for none, snappy or gzip. 
 */
public enum Codecs {
    None(Constants.NOOP, Constants.NONE),
    GzipCompressor(Constants.GZIP_CODEC, Constants.APPLICATION_X_GZIP), 
    SnappyCompressor(Constants.SNAPPY_CODEC, Constants.APPLICATION_X_SNAPPY_FRAMED);

    @Getter
    private final Codec codec;
    @Getter
    private final String mimeType;
    
    Codecs(Codec codec, String mimeType) {
        this.codec = codec;  
        this.mimeType = mimeType;
    }

    private static class Noop implements Codec {
        @Override
        public String getCodecType() {
            return Constants.NONE;
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
        @Override
        public String getCodecType() {
            return Constants.APPLICATION_X_GZIP;
        }
        
        @Override
        public ByteBuffer encode(ByteBuffer data) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(data.remaining());
            GZIPOutputStream gzipOS = new GZIPOutputStream(bos);
            gzipOS.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
            gzipOS.close();
            byte[] compressed = bos.toByteArray();
            return ByteBuffer.wrap(compressed);
        }

        @Override
        public ByteBuffer decode(ByteBuffer data) throws IOException {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            ByteArrayInputStream bis = new ByteArrayInputStream(array);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPInputStream gzipIS = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIS.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            byte[] uncompressed = bos.toByteArray();
            return ByteBuffer.wrap(uncompressed);
        }
    }

    private static class SnappyCodec implements Codec {
        @Override
        public String getCodecType() {
            return Constants.APPLICATION_X_SNAPPY_FRAMED;
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
