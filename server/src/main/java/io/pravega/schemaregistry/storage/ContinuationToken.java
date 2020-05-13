/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import com.google.common.base.Strings;

public class ContinuationToken {
    public static final ContinuationToken EMPTY = new ContinuationToken("");
    private final String token;

    private ContinuationToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return token;
    }

    public static ContinuationToken create(String token) {
        return fromString(token);
    }

    public static ContinuationToken fromString(String token) {
        if (Strings.isNullOrEmpty(token)) {
            return EMPTY;
        } else {
            return new ContinuationToken(token);
        }
    }
}
