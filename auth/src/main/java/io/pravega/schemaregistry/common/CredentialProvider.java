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

import lombok.AllArgsConstructor;

/**
 * This interface represents the credentials passed to Schema registry client. 
 */
public interface CredentialProvider {
    /**
     * Returns the authentication method to be used.
     *
     * @return the authentication method for these credentials.
     */
    String getMethod();

    /**
     * Returns the token to be sent to Schema registry service.
     * 
     * @return A token string.
     */
    String getToken();
    
    @AllArgsConstructor
    class DefaultCredentialProvider implements CredentialProvider {
        private final String authMethod;
        private final String authToken;
        
        @Override
        public String getMethod() {
            return authMethod;
        }

        @Override
        public String getToken() {
            return authToken;
        }
    }
}
