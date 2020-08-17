/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.LoggerHelpers;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.UriBuilder;

import io.pravega.common.security.JKSHelper;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.server.rest.filter.AuthenticationFilter;
import io.pravega.schemaregistry.server.rest.filter.NamespaceRedirectFilter;
import io.pravega.schemaregistry.server.rest.resources.GroupResourceImpl;
import io.pravega.schemaregistry.server.rest.resources.PingImpl;
import io.pravega.schemaregistry.server.rest.resources.SchemaResourceImpl;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RestServer extends AbstractIdleService {
    private final String objectId;
    private final ServiceConfig restServerConfig;
    private final URI baseUri;
    private final ResourceConfig resourceConfig;
    private HttpServer httpServer;

    public RestServer(SchemaRegistryService registryService, ServiceConfig restServerConfig) {
        this.objectId = "RestServer";
        this.restServerConfig = restServerConfig;
        this.baseUri = UriBuilder.fromUri("http://" + restServerConfig.getHost()).port(restServerConfig.getPort()).build();

        final Set<Object> resourceObjs = new HashSet<>();
        AuthHandlerManager authManager = new AuthHandlerManager(restServerConfig);
        resourceObjs.add(new AuthenticationFilter(restServerConfig.isAuthEnabled(), authManager));
        resourceObjs.add(new NamespaceRedirectFilter());
        resourceObjs.add(new PingImpl());
        resourceObjs.add(new GroupResourceImpl(registryService, restServerConfig, authManager, executor()));
        resourceObjs.add(new SchemaResourceImpl(registryService, restServerConfig, authManager, executor()));

        final RegistryApplication application = new RegistryApplication(resourceObjs);
        this.resourceConfig = ResourceConfig.forApplication(application);
        this.resourceConfig.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        this.resourceConfig.register(new CustomJsonParser());
    }

    /**
     * Start REST service.
     */
    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting REST server listening on port: {}", this.restServerConfig.getPort());
            if (restServerConfig.isTlsEnabled()) {
                SSLContextConfigurator contextConfigurator = new SSLContextConfigurator();
                contextConfigurator.setKeyStoreFile(restServerConfig.getTlsKeyStoreFilePath());
                contextConfigurator.setKeyStorePass(JKSHelper.loadPasswordFrom(restServerConfig.getTlsKeyStorePasswordFilePath()));
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true,
                        new SSLEngineConfigurator(contextConfigurator, false, false, false));
            } else {
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true);
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    /**
     * Gracefully stop REST service.
     */
    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping REST server listening on port: {}", this.restServerConfig.getPort());
            final GrizzlyFuture<HttpServer> shutdown = httpServer.shutdown(30, TimeUnit.SECONDS);
            log.info("Awaiting termination of REST server");
            shutdown.get();
            log.info("REST server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
