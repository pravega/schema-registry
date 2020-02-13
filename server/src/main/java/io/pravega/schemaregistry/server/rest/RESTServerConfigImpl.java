/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
public class RESTServerConfigImpl {
    private final String host;
    private final int port;

    @Builder
    RESTServerConfigImpl(final String host, final int port) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");

        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("host: %s, ", host))
                .append(String.format("port: %d, ", port))
                .toString();
    }
}
