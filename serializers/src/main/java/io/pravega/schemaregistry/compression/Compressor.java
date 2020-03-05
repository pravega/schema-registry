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

import java.nio.ByteBuffer;

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
}
