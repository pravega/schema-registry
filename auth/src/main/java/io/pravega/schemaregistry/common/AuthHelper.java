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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthenticationException;

public class AuthHelper {
    /**
     * Encodes auth method and auth token with as whitespace separated string. 
     * 
     * @param method Auth method.
     * @param token Auth token.
     * @return Encoded method and token.
     */
    public static String getAuthorizationHeader(String method, String token) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(method));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(token));
        return String.format("%s %s", method, token);
    }

    /**
     * Extracts method and token from credentials created using {@link AuthHelper#getAuthorizationHeader(String, String)}. 
     * 
     * @param credentials credentials
     * @return Array of method and token. 
     * @throws AuthenticationException throws authentication exception for malformed credentials. 
     */
    public static String[] extractMethodAndToken(String credentials) throws AuthenticationException {
        String[] parts = credentials.split("\\s+", 2);
        if (parts.length != 2) {
            throw new AuthenticationException("Malformed request");
        }
        return parts;
    }

}
