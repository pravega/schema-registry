/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.common;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashUtil {
    private static final HashFunction HASH = Hashing.murmur3_128();

    /**
     * Computes a 64 bit hash of supplied bytes using 128 bit murmur3 hash function and taking its first 8 bytes.
     *
     * @param bytes bytes to compute hash of. 
     * @return a 64 bit hash of the given bytes.
     */
    public static long getFingerprint(byte[] bytes) {
        return HASH.hashBytes(bytes).asLong();
    }
}
