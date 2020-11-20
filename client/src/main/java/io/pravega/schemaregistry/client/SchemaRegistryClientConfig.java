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

import io.pravega.schemaregistry.common.CredentialProvider;
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
     * CredentialProvider to be used for authentication and authorization. 
     */
    private final CredentialProvider credentialProvider;
    
    /*
     * Path to trust store for TLS server authentication certificate.
     */
    private final String trustStore;
    
    /**
     * Type of key store used as the trust store - e.g. jks, pkcs11, pkcs12, dks etc. If not specified then either 
     * certificate (if configured) or default java TLS store as specified in system properties would be used. 
     */
    private final String trustStoreType;
    
    /**
     * Password for the trust store. Defaults to null. 
     */
    private final String trustStorePassword;
    
    /**
     * If the trust store is a certificate file, typically DER or PEM file.  
     */
    private final String certificate;
    
    /**
     * Flag to indicate whether client should perform host name validation in server authentication certificate.
     */
    private final boolean validateHostName;

    private SchemaRegistryClientConfig(URI schemaRegistryUri, boolean authEnabled, CredentialProvider credentialProvider,
                                       String trustStore, String trustStoreType, String trustStorePassword, 
                                       String certificate, boolean validateHostName) {
        this.schemaRegistryUri = schemaRegistryUri;
        this.authEnabled = authEnabled;
        this.credentialProvider = credentialProvider;
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
                       .credentialProvider(new CredentialProvider.DefaultCredentialProvider(authMethod, authToken));
        }

        public SchemaRegistryClientConfigBuilder authentication(CredentialProvider credentialProvider) {
            return this.authEnabled()
                       .credentialProvider(credentialProvider);
        }

        private SchemaRegistryClientConfigBuilder authEnabled() {
            this.authEnabled = true;
            return this;
        }
    }
}
