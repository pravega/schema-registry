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
import io.pravega.client.stream.impl.Credentials;
import io.pravega.schemaregistry.common.CredentialProvider;
import lombok.AllArgsConstructor;

/**
 * Credential provider that is a wrapper over and uses pravega credentials to authenticate to schema registry.  
 */
@AllArgsConstructor
public class PravegaCredentialProvider implements CredentialProvider {
    private final Credentials credentials;

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
