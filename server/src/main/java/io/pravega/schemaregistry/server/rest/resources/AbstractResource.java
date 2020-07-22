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
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthContext;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.StoreExceptions;
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
import java.util.concurrent.CompletionException;
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
    
    CompletableFuture<Response> withAuthorization(AuthHandler.Permissions permissions,
                                                  String resource, AsyncResponse response,
                                                  Supplier<CompletableFuture<Response>> future,
                                                  SecurityContext securityContext, 
                                                  Supplier<String> logSupplier) {
        return CompletableFuture.runAsync(() -> authorize(securityContext, resource, permissions), executorService)
                         .thenCompose(v -> future.get())
                         .exceptionally(e -> {
                             Throwable unwrap = Exceptions.unwrap(e);
                             response.resume(handleExceptions(unwrap, logSupplier));
                             throw new CompletionException(e);
                         });
    }

    private boolean authorize(SecurityContext securityContext, String resource, AuthHandler.Permissions permission)
            throws AuthException {
        if (config.isAuthEnabled()) {
            AuthContext context = getAuthManager().getContext(securityContext);
            if (!context.authorize(resource, permission)) {
                throw new AuthorizationException(
                        String.format("Failed to authorize for resource [%s]", resource),
                        Response.Status.FORBIDDEN.getStatusCode());
            }
        }
        return true;
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

    Response handleExceptions(Throwable unwrap, Supplier<String> logSupplier) {
        Response response;
        if (unwrap instanceof AuthException) {
            log.warn("Auth failed for request {}.", logSupplier.get(), unwrap);
            response = Response.status(Response.Status.fromStatusCode(((AuthException) unwrap).getResponseCode())).build();
        } else if (unwrap instanceof IllegalArgumentException) {
            log.warn("Bad argument for request {}. {}. {}", logSupplier.get(), unwrap.getMessage());
            response = Response.status(Response.Status.BAD_REQUEST).entity(unwrap.getMessage()).build();
        } else if (unwrap instanceof PreconditionFailedException) {
            log.warn("Request {} failed. Precondition failed.", logSupplier.get());
            response = Response.status(Response.Status.CONFLICT).build();
        } else if (unwrap instanceof IncompatibleSchemaException) {
            log.warn("Request {} failed with Incompatible Schema.", logSupplier.get());
            response = Response.status(Response.Status.CONFLICT).build();
        } else if (unwrap instanceof StoreExceptions.DataNotFoundException || unwrap instanceof StoreExceptions.DataContainerNotFoundException) {
            log.warn("Request {} failed with resource not found. ", logSupplier.get());
            response = Response.status(Response.Status.NOT_FOUND).build();
        } else if (unwrap instanceof SerializationFormatMismatchException) {
            log.warn("Request {} failed with SerializationFormat Mismatch.", logSupplier.get());
            response = Response.status(Response.Status.EXPECTATION_FAILED).entity(unwrap.getMessage()).build();
        } else {
            log.warn("Request {} failed with Internal Server error.", logSupplier.get(), unwrap);
            response = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(unwrap.getMessage()).build();
        } 
        return response;
    }
}
