/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.filter;

import com.google.common.base.Preconditions;
import io.pravega.auth.AuthenticationException;
import io.pravega.schemaregistry.server.rest.auth.AuthContext;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.util.List;

/**
 * Authentication filter. 
 * Performs authentication using the Auth Handler infrastructure and sets the principal in the Security Context. 
 */
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
@Slf4j
public class AuthenticationFilter implements ContainerRequestFilter {
    @Context
    HttpHeaders headers;

    private final boolean authEnabled;
    private final AuthHandlerManager authManager;

    public AuthenticationFilter(boolean authEnabled, AuthHandlerManager authManager) {
        this.authEnabled = authEnabled;
        this.authManager = authManager;
    }

    @Override
    public void filter(ContainerRequestContext containerRequest)
            throws WebApplicationException {
        if (authEnabled) {
            String credentials = parseCredentials(headers.getRequestHeader(HttpHeaders.AUTHORIZATION));
            try {
                AuthContext context = authManager.getContext(credentials);
                context.authenticate();
                containerRequest.setSecurityContext(context);
            } catch (AuthenticationException e) {
                log.warn("User authentication failed");
                containerRequest.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());            
            }
        }
    }

    private String parseCredentials(List<String> authHeader) throws AuthenticationException {
        if (authHeader == null || authHeader.isEmpty()) {
            throw new AuthenticationException("Missing authorization header.");
        }

        // Expecting a single value here. If there are multiple, we'll deal with just the first one.
        String credentials = authHeader.get(0);
        Preconditions.checkNotNull(credentials, "Missing credentials.");
        return credentials;
    }
}