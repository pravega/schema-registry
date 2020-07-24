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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Decoder interface that defines method to decode data.
 */
@FunctionalInterface
public interface Decoder {
    /**
     * Implementation should decode the remaining bytes in the buffer and return a new ByteBuffer that includes
     * the decoded data at its current position. 
     *
     * @param data encoded ByteBuffer to decode. 
     * @param codecProperties codec properties. 
     * @return decoded ByteBuffer with position set to the start of decoded data. 
     * @throws IOException can be thrown while reading from or writing to byte buffers.
     */
    ByteBuffer decode(ByteBuffer data, Map<String, String> codecProperties) throws IOException;
}
