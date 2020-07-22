/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.auth;

import com.google.common.base.Preconditions;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages instances of {@link AuthHandler}.
 */
@Slf4j
public class AuthContext implements SecurityContext {
    private final AuthHandler handler;
    private final String token;
    private final AtomicReference<Principal> principal = new AtomicReference<>();

    AuthContext(AuthHandler handler, String token) {
        this.handler = handler;
        this.token = token;
    }

    AuthContext(AuthHandler handler, Principal principal) {
        this.handler = handler;
        this.principal.set(principal);
        this.token = null;
    }

    /**
     * Authenticate the user using the token.
     *
     * @throws AuthenticationException if an authentication failure occurred.
     */
    public void authenticate() throws AuthenticationException {
        principal.compareAndSet(null, handler.authenticate(token));
    }

    /**
     * Authorize user for permissions on the resource with ACLs. It is important to have called authenticate
     * before calling authorize. 
     *
     * @param resource resource to check authorization on. 
     * @param level ACLs. 
     * @return true if the user is authorized, false otherwise. 
     */
    public boolean authorize(String resource, AuthHandler.Permissions level) {
        Preconditions.checkNotNull(resource);
        Preconditions.checkNotNull(level);
        Preconditions.checkNotNull(principal.get(), "Authentication should have been called before authorization");
        return handler.authorize(resource, principal.get()).ordinal() >= level.ordinal();
    }

    @Override
    public Principal getUserPrincipal() {
        return principal.get();
    }

    @Override
    public boolean isUserInRole(String role) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSecure() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAuthenticationScheme() {
        return handler.getHandlerName();
    }
}
