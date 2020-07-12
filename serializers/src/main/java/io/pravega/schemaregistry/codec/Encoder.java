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

import io.pravega.schemaregistry.contract.data.CodecType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Defines method to encode data.
 */
public interface Encoder {
    /**
     * Codec type for the encoder. 
     * 
     * @return Codec Type for the encoder. 
     */
    CodecType getCodecType();
    
    /**
     * Implementation should encode the remaining bytes in the buffer and return a new ByteBuffer that includes
     * the encoded data at its current position. 
     * 
     * @param data ByteBuffer to encode. 
     * @return encoded ByteBuffer with position set to the start of encoded data. 
     * @throws IOException IOException can be thrown while reading from or writing to byte buffers.
     */
    ByteBuffer encode(ByteBuffer data) throws IOException;
}
