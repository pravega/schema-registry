/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import lombok.Builder;
import lombok.Data;

import java.net.URI;

/**
 * Registry client configuration used to create registry client. 
 */
@Data
@Builder
public class SchemaRegistryClientConfig {
    /**
     * URI for connecting with registry client.
     */
    private final URI schemaRegistryUri;
    /**
     * Flag to indicate if authentication is enabled.
     */
    private final boolean authEnabled;
    /**
     * Authentication method.
     */
    private final String authMethod;
    /**
     * Authentication token.
     */
    private final String authToken;
    /*
     * Path to trust store for TLS server authentication certificate.
     */
    private final String trustStore;
    /**
     * If the trust store is a certificate file, typically DER or PEM file.  
     */
    private final String certificate;
    /**
     * Type of trust store - This should either be a certificate, key store in jks or pkcs12 format. 
     */
    private final String trustStoreType;
    /**
     * Password for the trust store. Defaults to null. 
     */
    private final String trustStorePassword;
    /**
     * Flag to indicate whether client should perform host name validation in server authentication certificate.
     */
    private final boolean validateHostName;

    private SchemaRegistryClientConfig(URI schemaRegistryUri, boolean authEnabled, String authMethod, String authToken,
                                       String trustStore, String certificate, String trustStoreType,
                                       String trustStorePassword, boolean validateHostName) {
        this.schemaRegistryUri = schemaRegistryUri;
        this.authEnabled = authEnabled;
        this.authMethod = authMethod;
        this.authToken = authToken;
        this.trustStore = trustStore;
        this.certificate = certificate;
        this.trustStoreType = trustStoreType;
        this.trustStorePassword = trustStorePassword;
        this.validateHostName = validateHostName;
    }

    public static final class SchemaRegistryClientConfigBuilder {
        private boolean authEnabled = false;
        private boolean validateHostName = false;
        private String trustStore = null;
        private String trustStoreType = null;
        private String trustStorePassword = null;
        private String certificate = null;

        public SchemaRegistryClientConfigBuilder certificate(String certificate) {
            this.certificate = certificate;
            return this;
        }

        public SchemaRegistryClientConfigBuilder trustStore(String trustStore, String trustStoreType, String trustStorePassword) {
            this.trustStore = trustStore;
            return this.trustStoreType(trustStoreType)
                       .trustStorePassword(trustStorePassword);
        }

        private SchemaRegistryClientConfigBuilder trustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        private SchemaRegistryClientConfigBuilder trustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public SchemaRegistryClientConfigBuilder authentication(String authMethod, String authToken) {
            return this.authEnabled()
                       .authMethod(authMethod)
                       .authToken(authToken);
        }

        private SchemaRegistryClientConfigBuilder authEnabled() {
            this.authEnabled = true;
            return this;
        }

        private SchemaRegistryClientConfigBuilder authMethod(String authMethod) {
            this.authMethod = authMethod;
            return this;
        }

        private SchemaRegistryClientConfigBuilder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }
    }
}
