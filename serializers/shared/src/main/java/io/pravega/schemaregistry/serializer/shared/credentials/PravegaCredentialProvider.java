/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.credentials;

import io.pravega.client.ClientConfig;
import io.pravega.schemaregistry.common.CredentialProvider;
import io.pravega.shared.security.auth.Credentials;

/**
 * Credential provider that is a wrapper over and uses pravega credentials to authenticate to schema registry.
 * This is to be used if users want to use the same credentials to authenticate to schema registry 
 * that they have used in pravega client.
 * This can be achieved in one of two ways - 
 * 1. instantiate this class with Pravega client config and this will extract credentials from the client config. 
 * 2. instantiate this class without any credentials and it will extract credentials from the environment
 * by creating a default clientconfig which loads the credentials from the environment variables or system properies. 
 * Refer to {@link ClientConfig} to see how credentials are loaded from the environment. 
 */
public class PravegaCredentialProvider implements CredentialProvider {
    private final Credentials credentials;

    public PravegaCredentialProvider(ClientConfig pravegaClientConfig) {
        this.credentials = pravegaClientConfig.getCredentials();
    }

    public PravegaCredentialProvider() {
        credentials = ClientConfig.builder().build().getCredentials();
    }

    @Override
    public String getMethod() {
        return credentials.getAuthenticationType();
    }

    @Override
    public String getToken() {
        return credentials.getAuthenticationToken();
    }
}
