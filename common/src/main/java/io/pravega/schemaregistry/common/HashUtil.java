/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {
    private static final String SHA_256 = "SHA-256";
    private static final MessageDigest MD;

    static {
        try {
            MD = MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Computes a 64 bit hash of supplied bytes using sha-256 hash function.
     *
     * @param bytes bytes to compute hash of. 
     * @return a 256 bit hash of the given bytes.
     */
    public static BigInteger getFingerprint(byte[] bytes) {
        return new BigInteger(MD.digest(bytes));
    }
}
