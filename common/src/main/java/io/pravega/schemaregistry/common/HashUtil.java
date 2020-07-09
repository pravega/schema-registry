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

import lombok.SneakyThrows;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {
    private static final String SHA_256 = "SHA-256";

    /**
     * Computes a 256 bit hash of supplied bytes using sha-256 hash function.
     *
     * @param bytes bytes to compute hash of. 
     * @return a 256 bit hash of the given bytes.
     */
    @SneakyThrows(NoSuchAlgorithmException.class)
    public static BigInteger getFingerprint(byte[] bytes) {
        MessageDigest md = MessageDigest.getInstance(SHA_256);

        return new BigInteger(md.digest(bytes));
    }
}
