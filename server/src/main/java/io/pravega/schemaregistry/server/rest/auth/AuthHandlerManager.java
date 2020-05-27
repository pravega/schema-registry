/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.controller.server.rpc.auth.PasswordAuthHandler;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static io.pravega.schemaregistry.common.AuthHelper.extractMethodAndToken;

/**
 * Manages instances of {@link AuthHandler}.
 */
@Slf4j
public class AuthHandlerManager {
    private final ServiceConfig serverConfig;

    @GuardedBy("this")
    private final Map<String, AuthHandler> handlerMap;

    public AuthHandlerManager(ServiceConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.handlerMap = new HashMap<>();
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

    private AuthHandler getHandler(String handlerName) throws AuthenticationException {
        AuthHandler retVal;
        synchronized (this) {
            retVal = handlerMap.get(handlerName);
        }
        if (retVal == null) {
            throw new AuthenticationException("Handler does not exist for method " + handlerName);
        }
        return retVal;
    }

    /**
     * API to authenticate and authorize access to a given resource.
     *
     * @param resource    The resource identifier for which the access needs to be controlled.
     * @param credentials Credentials used for authentication.
     * @param level       Expected level of access.
     * @return Returns true if the entity represented by the custom auth headers had given level of access to the resource.
     * Returns false if the entity does not have access.
     * @throws AuthenticationException if an authentication failure occurred.
     */
    public boolean authenticateAndAuthorize(String resource, String credentials, AuthHandler.Permissions level) throws AuthenticationException {
        Preconditions.checkNotNull(credentials, "credentials");
        boolean retVal = false;
        try {
            String[] parts = extractMethodAndToken(credentials);
            String method = parts[0];
            String token = parts[1];
            AuthHandler handler = getHandler(method);
            Preconditions.checkNotNull(handler, "Can not find handler.");
            Principal principal;
            if ((principal = handler.authenticate(token)) == null) {
                throw new AuthenticationException("Authentication failure");
            }
            retVal = handler.authorize(resource, principal).ordinal() >= level.ordinal();
        } catch (AuthException e) {
            throw new AuthenticationException("Authentication failure");
        }
        return retVal;
    }
    
    /**
     * This method is not only visible for testing, but also intended to be used solely for testing. It allows tests
     * to inject and register custom auth handlers. Also, this method is idempotent.
     *
     * @param authHandler the {@code AuthHandler} implementation to register
     * @throws NullPointerException {@code authHandler} is null
     */
    @VisibleForTesting
    @Synchronized
    void registerHandler(AuthHandler authHandler) {
        Preconditions.checkNotNull(authHandler, "authHandler");
        this.handlerMap.put(authHandler.getHandlerName(), authHandler);
    }
}
