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

import lombok.SneakyThrows;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Factory class for creating codecs for codec types . 
 */
public class CodecFactory {
    public static final String NONE = "";
    public static final String MIME_GZIP = "application/x-gzip";
    public static final String MIME_SNAPPY = "application/x-snappy-framed";

    private static final Noop NOOP = new Noop();
    private static final GZipCodec GZIP_COMPRESSOR = new GZipCodec();
    private static final SnappyCodec SNAPPY_COMPRESSOR = new SnappyCodec();
    
    public static Codec none() {
        return NOOP;
    }

    public static Codec gzip() {
        return GZIP_COMPRESSOR;
    }

    public static Codec snappy() {
        return SNAPPY_COMPRESSOR;
    }

    private static class Noop implements Codec {
        @Override
        public String getCodecType() {
            return NONE;
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
            return MIME_GZIP;
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer encode(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            ByteArrayOutputStream bos = new ByteArrayOutputStream(array.length);
            GZIPOutputStream gzipOS = new GZIPOutputStream(bos);
            gzipOS.write(array);
            gzipOS.close();
            byte[] compressed = bos.toByteArray();
            return ByteBuffer.wrap(compressed);
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer decode(ByteBuffer data) {
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
            return MIME_SNAPPY;
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer encode(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            byte[] compressed = Snappy.compress(array);
            return ByteBuffer.wrap(compressed);
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer decode(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            byte[] uncompressed = Snappy.uncompress(array);
            return ByteBuffer.wrap(uncompressed);
        }
    }

}
