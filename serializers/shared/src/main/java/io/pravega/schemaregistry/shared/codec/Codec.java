/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.shared.codec;

import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingInfo;

/**
 * Codec interface extends {@link Encoder} and {@link Decoder} interfaces that defines methods to encode and decode
 * data. Encoder interface takes a codec type and encoding function. Decoder interface defines a decoding function. 
 */
public interface Codec extends Encoder, Decoder {
    /**
     * Name identifying the Codec Type. 
     * This name should be same as the {@link CodecType#getName()} that is registered for the group in schema registry 
     * service. 
     * The deserializers will find the decoder for the encoded data from {@link EncodingInfo#getCodecType()} by matching 
     * the name.
     * 
     * @return Name of the codec. 
     */
    String getName();
}
