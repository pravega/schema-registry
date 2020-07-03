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

import io.pravega.schemaregistry.contract.data.EncodingInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Codec interface that defines methods to encode and decoder data for a given codec type.
 */
public interface Codec {
    /**
     * String name identifying the Codec Type. This should be same as the codecType that is registered for the group 
     * in schema registry service. The serializers will use this codec to encode the data and deserializers will find
     * the decoder for the encoded data from {@link EncodingInfo#codecType}
     * 
     * @return Name of the codec. 
     */
    String getCodecType();

    /**
     * Implementation should encode the remaining bytes in the buffer and return a new ByteBuffer that includes
     * the encoded data at its current position. 
     * 
     * @param data ByteBuffer to encode. 
     * @return encoded ByteBuffer with position set to the start of encoded data. 
     * @throws IOException IOException can be thrown while reading from or writing to byte buffers.
     */
    ByteBuffer encode(ByteBuffer data) throws IOException;

    /**
     * Implementation should decode the remaining bytes in the buffer and return a new ByteBuffer that includes
     * the decoded data at its current position. 
     *
     * @param data encoded ByteBuffer to decode. 
     * @return decoded ByteBuffer with position set to the start of decoded data. 
     * @throws IOException can be thrown while reading from or writing to byte buffers.
     */
    ByteBuffer decode(ByteBuffer data) throws IOException;
}
