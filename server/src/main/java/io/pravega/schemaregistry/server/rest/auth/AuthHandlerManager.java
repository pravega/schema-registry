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
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.controller.server.security.auth.handler.impl.PasswordAuthHandler;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.SecurityContext;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

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
                    if (handler instanceof PasswordAuthHandler && !(handler instanceof BasicAuthHandler)) {
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
     * @param credentials CredentialProvider to use. 
     * @return Context object that can be used to perform authentication and authorization for supplied credentials
     * @throws AuthenticationException if the handler is not registered. 
     */
    public AuthContext getContext(String credentials) throws AuthenticationException {
        AuthHandler handler;
        String[] parts = extractMethodAndToken(credentials);
        String method = parts[0];
        String token = parts[1];

        handler = handlerMap.get(method);
        if (handler == null) {
            throw new AuthenticationException("Handler does not exist for method " + method);
        }

        return new AuthContext(handler, token);
    }

    /**
     * Get auth context for the credentials. It extracts method and token from the credentials
     * and loads the auth handler corresponding to the method. 
     * Subsequently, authentication and authorization can be called on the context which will use the auth handler
     * and then token from the credentials to authenticate and authorize. 
     * 
     * @param securityContext Security Context. 
     * @return Context object that can be used to perform authentication and authorization for supplied credentials
     * @throws AuthenticationException if the handler 
     */
    public AuthContext getContext(SecurityContext securityContext) throws AuthenticationException {
        AuthHandler handler = handlerMap.get(securityContext.getAuthenticationScheme());

        if (handler == null) {
            throw new AuthenticationException("Handler does not exist for method " + securityContext.getAuthenticationScheme());
        }

        return new AuthContext(handler, securityContext.getUserPrincipal());
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
}
