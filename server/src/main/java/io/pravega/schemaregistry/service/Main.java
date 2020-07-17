/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.google.common.base.Strings;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;
import io.pravega.schemaregistry.storage.StoreType;
import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

@Slf4j
public class Main {
    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(Config.THREAD_POOL_SIZE);
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(Config.PRAVEGA_CONTROLLER_URI))
                                                .trustStore(Config.PRAVEGA_TLS_TRUST_STORE)
                                                .validateHostName(Config.PRAVEGA_TLS_VALIDATE_HOSTNAME)
                                                .credentials(getCredentials())
                                                .build();

        SchemaStore schemaStore;
        ServiceConfig serviceConfig = Config.SERVICE_CONFIG;
        if (Config.STORE_TYPE.equals(StoreType.Pravega.name())) {
            schemaStore = SchemaStoreFactory.createPravegaStore(clientConfig, executor);
        } else if (Config.STORE_TYPE.equals(StoreType.InMemory.name())) {
            schemaStore = SchemaStoreFactory.createInMemoryStore(executor);
        } else {
            throw new IllegalArgumentException(String.format("Store Type %s not supported", Config.STORE_TYPE));
        }
        
        SchemaRegistryService service = new SchemaRegistryService(schemaStore, executor);

        setUncaughtExceptionHandler(Main::logUncaughtException);

        RestServer restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        log.info("Awaiting start of REST server");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            onShutdown(restServer, executor);
        }));

        restServer.awaitRunning();
        restServer.awaitTerminated();
        System.exit(0);
    }

    private static Credentials getCredentials() {
        if (!Strings.isNullOrEmpty(Config.PRAVEGA_CREDENTIALS_AUTH_METHOD)) {
            return new Credentials() {
                @Override
                public String getAuthenticationType() {
                    return Config.PRAVEGA_CREDENTIALS_AUTH_METHOD;
                }

                @Override
                public String getAuthenticationToken() {
                    return Config.PRAVEGA_CREDENTIALS_AUTH_TOKEN;
                }
            };
        } else {
            return null;
        }
    }

    private static void setUncaughtExceptionHandler(BiConsumer<Thread, Throwable> exceptionConsumer) {
        Thread.setDefaultUncaughtExceptionHandler(exceptionConsumer::accept);
    }

    private static void logUncaughtException(Thread t, Throwable e) {
        log.error("Thread {} with stackTrace {} failed with uncaught exception", t.getName(), t.getStackTrace(), e);
    }

    private static void onShutdown(RestServer restServer, ScheduledExecutorService executor) {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        memoryMXBean.setVerbose(true);
        log.info("Shutdown hook memory usage dump: Heap memory usage: {}, non heap memory usage {}", memoryMXBean.getHeapMemoryUsage(),
                memoryMXBean.getNonHeapMemoryUsage());

        try {
            restServer.stopAsync();
            restServer.awaitTerminated();
        } finally {
            Thread.getAllStackTraces().forEach((key, value) ->
                    log.info("Shutdown Hook Thread dump: Thread {} stackTrace: {} ", key.getName(), value));
        }

        ExecutorServiceHelpers.shutdown(executor);
    }
}
