/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthorizationException;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static io.pravega.schemaregistry.server.rest.resources.AuthResources.DEFAULT_NAMESPACE;
import static io.pravega.schemaregistry.server.rest.resources.AuthResources.NAMESPACE_GROUP_FORMAT;
import static io.pravega.schemaregistry.server.rest.resources.AuthResources.DOMAIN;
import static io.pravega.schemaregistry.server.rest.resources.AuthResources.NAMESPACE_FORMAT;
import static io.pravega.schemaregistry.server.rest.resources.AuthResources.NAMESPACE_GROUP_CODEC_FORMAT;
import static io.pravega.schemaregistry.server.rest.resources.AuthResources.NAMESPACE_GROUP_SCHEMA_FORMAT;

@Slf4j
abstract class AbstractResource {
    @Context
    HttpHeaders headers;

    @Getter
    private final SchemaRegistryService registryService;
    @Getter
    private final ServiceConfig config;
    @Getter
    private final AuthHandlerManager authManager;
    @Getter
    private final Executor executorService;

    AbstractResource(SchemaRegistryService registryService, ServiceConfig config, AuthHandlerManager authManager, Executor executorService) {
        this.registryService = registryService;
        this.config = config;
        this.authManager = authManager;
        this.executorService = executorService;
    }
    
    CompletableFuture<Response> withAuthorization(String request, AuthHandler.Permissions permissions,
                                                  String resource, AsyncResponse response,
                                                  Supplier<CompletableFuture<Response>> future,
                                                  SecurityContext securityContext) {
        try {
            authorize(securityContext, resource, permissions);
            return future.get();
        } catch (AuthException e) {
            log.warn("Auth failed {}", request, e);
            response.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            throw e;
        } catch (IllegalArgumentException e) {
            log.warn("Bad request {} error:{}", request, e.getMessage());
            return CompletableFuture.completedFuture(Response.status(Response.Status.BAD_REQUEST).build());
        } catch (Exception e) {
            log.error("request failed with exception {}", e);
            return Futures.failedFuture(e);
        }
    }

    private void authorize(SecurityContext securityContext, String resource, AuthHandler.Permissions permission)
            throws AuthException {
        if (config.isAuthEnabled()) {
            AuthHandlerManager.Context context = (AuthHandlerManager.Context) securityContext;
            if (!context.authorize(resource, permission)) {
                throw new AuthorizationException(
                        String.format("Failed to authorize for resource [%s]", resource),
                        Response.Status.FORBIDDEN.getStatusCode());
            }
        }
    }

    String getNamespaceResource() {
        return DOMAIN;
    }

    String getNamespaceResource(String namespace) {
        String encode = null;
        try {
            encode = namespace == null ? "" : URLEncoder.encode(namespace, Charsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Invalid namespace name.");
        }
        return String.format(NAMESPACE_FORMAT, encode);
    }

    String getGroupResource(String group) {
        return getGroupResource(group, DEFAULT_NAMESPACE);
    }

    String getGroupResource(String group, String namespace) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        try {
            String encodedNamespace = namespace == null ? "" : URLEncoder.encode(namespace, Charsets.UTF_8.toString());
            return String.format(NAMESPACE_GROUP_FORMAT, encodedNamespace,
                    URLEncoder.encode(group, Charsets.UTF_8.toString()));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Invalid group or namespace name.");
        }
    }

    String getGroupSchemaResource(String group) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        return getGroupSchemaResource(group, DEFAULT_NAMESPACE);
    }

    String getGroupSchemaResource(String group, String namespace) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        try {
            String encodedNamespace = namespace == null ? "" : URLEncoder.encode(namespace, Charsets.UTF_8.toString());

            return String.format(NAMESPACE_GROUP_SCHEMA_FORMAT,
                    encodedNamespace, URLEncoder.encode(group, Charsets.UTF_8.toString()));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Invalid group or namespace name.");
        }
    }

    String getGroupCodecResource(String group) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        return getGroupCodecResource(group, DEFAULT_NAMESPACE);
    }

    String getGroupCodecResource(String group, String namespace) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group));
        try {
            String encodedNamespace = namespace == null ? "" : URLEncoder.encode(namespace, Charsets.UTF_8.toString());

            return String.format(NAMESPACE_GROUP_CODEC_FORMAT,
                    encodedNamespace, URLEncoder.encode(group, Charsets.UTF_8.toString()));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Invalid group or namespace name.");
        }
    }
}
