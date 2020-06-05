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

import java.nio.ByteBuffer;

/**
 * Codec interface that defines methods to encode and decoder data for a given codec type.
 */
public interface Codec {
    String getCodecType();

    ByteBuffer encode(ByteBuffer data);

    ByteBuffer decode(ByteBuffer data);
}
