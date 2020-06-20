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
import io.pravega.auth.ServerConfig;
import io.pravega.common.Exceptions;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
@Builder
public class ServiceConfig implements ServerConfig {
    private final String host;
    private final int port;
    private final boolean tlsEnabled;
    private final String tlsCertFile;
    private final String tlsTrustStore;
    private final String tlsKeyFilePath;
    private final String tlsKeyPasswordFilePath;
    private final boolean authEnabled;
    private final String userPasswordFile;

    private ServiceConfig(String host, int port, boolean tlsEnabled, String tlsCertFile, String tlsTrustStore,
                          String tlsKeyFilePath, String tlsKeyPasswordFilePath, boolean authEnabled, String userPasswordFile) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");
        Exceptions.checkArgument(!tlsEnabled || (!Strings.isNullOrEmpty(tlsCertFile) &&
                        !Strings.isNullOrEmpty(tlsKeyFilePath) && !Strings.isNullOrEmpty(tlsTrustStore)), "keyFilePath", 
                "If tls is enabled then key file path and key file password path should be non empty");
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertFile = tlsCertFile;
        this.tlsKeyFilePath = tlsKeyFilePath;
        this.tlsKeyPasswordFilePath = tlsKeyPasswordFilePath;
        this.tlsTrustStore = tlsTrustStore;
        this.authEnabled = authEnabled;
        this.userPasswordFile = userPasswordFile;
    }

    public static final class ServiceConfigBuilder {
        private String host = "0.0.0.0";
        private int port = 9092;
        private boolean tlsEnabled = false;
        private boolean authEnabled = false;
    }
    
    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("host: %s, ", host))
                .append(String.format("port: %d, ", port))
                .append(String.format("authEnabled: %b, ", authEnabled))
                .toString();
    }
}
