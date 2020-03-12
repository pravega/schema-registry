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

/**
 * Compression factory class for creating compressor. 
 * This has implementation for {@link Compressor} for {@link CompressionType#None}
 */
public class CompressionFactory {
    public static Compressor getCompressor(CompressionType compressionType) {
        switch (compressionType) {
            case None:
                return new Compressor.Noop();
            case Snappy: 
            case GZip: 
            default:
                throw new RuntimeException("not implemented");
        }
    }
}
