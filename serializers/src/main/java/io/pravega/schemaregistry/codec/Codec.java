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

import java.nio.ByteBuffer;

/**
 * Codec interface that defines methods to encode and decode data for a given {@link CodecType}.
 * Currently we only have implementation for {@link CodecType#None}
 */
public interface Codec {
    CodecType getCodecType();

    ByteBuffer encode(ByteBuffer data);

    ByteBuffer decode(ByteBuffer data);
}
