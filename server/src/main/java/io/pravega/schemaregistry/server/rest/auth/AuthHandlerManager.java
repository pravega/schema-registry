/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.controller.server.rpc.auth.PasswordAuthHandler;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import lombok.extern.slf4j.Slf4j;

import java.security.Principal;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.schemaregistry.common.AuthHelper.extractMethodAndToken;

/**
 * Manages instances of {@link AuthHandler}.
 */
@Slf4j
public class AuthHandlerManager {
    private final ServiceConfig serverConfig;

    private final ConcurrentHashMap<String, AuthHandler> handlerMap;

    public AuthHandlerManager(ServiceConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.handlerMap = new ConcurrentHashMap<>();
        this.loadHandlers();
    }

    private void loadHandlers() {
        try {
            if (serverConfig.isAuthEnabled()) {
                ServiceLoader<AuthHandler> loader = ServiceLoader.load(AuthHandler.class);
                for (AuthHandler handler : loader) {
                    if (handler instanceof PasswordAuthHandler) {
                        continue;
                    }
                    try {
                        handler.initialize(serverConfig);
                        registerHandler(handler);
                    } catch (Exception e) {
                        log.warn("Exception while initializing auth handler {}", handler, e);
                    }
                }
            }
        } catch (Throwable e) {
            log.warn("Exception while loading the auth handlers", e);
        }
    }

    /**
     * Get auth context for the credentials. It extracts method and token from the credentials
     * and loads the auth handler corresponding to the method. 
     * Subsequently, authentication and authorization can be called on the context which will use the auth handler
     * and then token from the credentials to authenticate and authorize. 
     * 
     * @param credentials Credentials to use. 
     * @return Context object that can be used to perform authentication and authorization for supplied credentials
     * @throws AuthenticationException if the handler 
     */
    public Context getContext(String credentials) throws AuthenticationException {
        AuthHandler handler;
        String[] parts = extractMethodAndToken(credentials);
        String method = parts[0];
        String token = parts[1];

        handler = handlerMap.get(method);
        if (handler == null) {
            throw new AuthenticationException("Handler does not exist for method " + method);
        }

        return new Context(handler, token);
    }
    
    /**
     * This method is not only visible for testing, but also intended to be used solely for testing. It allows tests
     * to inject and register custom auth handlers. Also, this method is idempotent.
     *
     * @param authHandler the {@code AuthHandler} implementation to register
     * @throws NullPointerException {@code authHandler} is null
     */
    @VisibleForTesting
    void registerHandler(AuthHandler authHandler) {
        Preconditions.checkNotNull(authHandler, "authHandler");
        this.handlerMap.put(authHandler.getHandlerName(), authHandler);
    }

    /**
     * Context class which sets the context for a request. It is derived by using the auth handler for the authentication 
     * method and stores the authentication token.  
     */
    public static class Context {
        private final AuthHandler handler;
        private final String token;
        private final AtomicReference<Principal> principal = new AtomicReference<>();

        private Context(AuthHandler handler, String token) {
            this.handler = handler;
            this.token = token;
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

        /**
         * API to authenticate and authorize access to a given resource.
         *
         * @param resource    The resource identifier for which the access needs to be controlled.
         * @param level       Expected level of access.
         * @return Returns true if the entity represented by the custom auth headers had given level of access to the resource.
         * Returns false if the entity does not have access.
         * @throws AuthenticationException if an authentication failure occurred.
         */
        public boolean authenticateAndAuthorize(String resource, AuthHandler.Permissions level) throws AuthenticationException {
            boolean retVal = false;
            try {
                authenticate();
                authorize(resource, level);
            } catch (AuthException e) {
                throw new AuthenticationException("Authentication failure");
            }
            return retVal;
        }
    }
}
