/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.compression;

import io.pravega.schemaregistry.contract.data.CompressionType;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.xerial.snappy.Snappy;

/**
 * Compressor interface that defines methods to compress and uncompress data for a given {@link CompressionType}.
 * Currently we only have implementation for {@link CompressionType#None}
 */
public interface Compressor {
    CompressionType getCompressionType();

    ByteBuffer compress(ByteBuffer data);

    ByteBuffer uncompress(ByteBuffer data);
    
    class Noop implements Compressor {
        @Override
        public CompressionType getCompressionType() {
            return CompressionType.None;
        }

        @Override
        public ByteBuffer compress(ByteBuffer data) {
            return data;
        }

        @Override
        public ByteBuffer uncompress(ByteBuffer data) {
            return data;
        }
    }

    class GZipCompressor implements Compressor {
        @Override
        public CompressionType getCompressionType() {
            return CompressionType.GZip;
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer compress(ByteBuffer data) {
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
        public ByteBuffer uncompress(ByteBuffer data) {
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
    
    class SnappyCompressor implements Compressor {
        @Override
        public CompressionType getCompressionType() {
            return CompressionType.Snappy;
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer compress(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            byte[] compressed = Snappy.compress(array);
            return ByteBuffer.wrap(compressed);
        }

        @SneakyThrows(IOException.class)
        @Override
        public ByteBuffer uncompress(ByteBuffer data) {
            byte[] array = new byte[data.remaining()];
            data.get(array);

            byte[] uncompressed = Snappy.uncompress(array);
            return ByteBuffer.wrap(uncompressed);
        }
    }
}
