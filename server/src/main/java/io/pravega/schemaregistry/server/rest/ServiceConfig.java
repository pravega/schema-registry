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
import lombok.ToString;

/**
 * REST server config.
 */
@Getter
@Builder
public class ServiceConfig implements ServerConfig {
    private final String host;
    private final int port;
    private final boolean tlsEnabled;
    @ToString.Exclude
    private final String tlsCertFilePath;
    @ToString.Exclude
    private final String serverKeyStoreFilePath;
    @ToString.Exclude
    private final String tlsKeyStorePasswordFilePath;
    private final boolean authEnabled;
    @ToString.Exclude
    private final String userPasswordFilePath;

    private ServiceConfig(String host, int port, boolean tlsEnabled, String tlsCertFilePath, 
                          String serverKeyStoreFilePath, String tlsKeyStorePasswordFilePath, boolean authEnabled, String userPasswordFilePath) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");
        Exceptions.checkArgument(!tlsEnabled || (!Strings.isNullOrEmpty(tlsCertFilePath) &&
                        !Strings.isNullOrEmpty(serverKeyStoreFilePath)), "keyFilePath", 
                "If tls is enabled then key file path and key file password path should be non empty");
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertFilePath = tlsCertFilePath;
        this.serverKeyStoreFilePath = serverKeyStoreFilePath;
        this.tlsKeyStorePasswordFilePath = tlsKeyStorePasswordFilePath;
        this.authEnabled = authEnabled;
        this.userPasswordFilePath = userPasswordFilePath;
    }

    public static final class ServiceConfigBuilder {
        private String host = "0.0.0.0";
        private int port = 9092;
        private boolean tlsEnabled = false;
        private boolean authEnabled = false;
    }
}
