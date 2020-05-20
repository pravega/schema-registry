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
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import io.pravega.schemaregistry.server.rest.ServiceConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * The configuration values are retrieved using the following order, with every one overriding previously loaded values:
 * 1. The configuration file. By default a 'schema-registry.config.properties' file is sought in the classpath; this can be
 * overridden by setting the 'conf.file' system property to point to another one.
 * 2. Environment Variables ({@link System#getenv()}).
 * 3. System Properties ({@link System#getProperties()}.
 * 4. All currently loaded values will be resolved against themselves.
 * 5. Anything which is not supplied via the methods above will be defaulted to the values defined in this class.
 *
 * Configuration values can be resolved against themselves by referencing them using a special syntax. Any value
 * of the form '${CFG}' will lookup the already loaded config value with key 'CFG' and, if defined and non-empty, it will
 * use that config value as the final value (if not defined or empty, it will not be included in the final result and the
 * default value (step 5) will be used). Chained resolution is not supported (i.e., CFG1=${CFG2};CFG2=${CFG3} will not
 * set CFG1 to the value of CFG3).
 */
@Slf4j
public final class Config {

    //#region Properties

    public static final String SERVICE_HOST;
    public static final int SERVICE_PORT;
    public static final String PRAVEGA_CONTROLLER_URI;
    public static final int THREAD_POOL_SIZE;
    public static final String STORE_TYPE;

    public static final boolean TLS_ENABLED;
    public static final String TLS_KEY_FILE;
    public static final String TLS_KEY_PASSWORD_FILE;
    public static final String TLS_CERT_FILE;
    public static final String TLS_TRUST_STORE;

    public static final boolean AUTHORIZATION_ENABLED;
    public static final String USER_PASSWORD_FILE;

    public static final ServiceConfig SERVICE_CONFIG;

    //endregion

    //region Property Definitions
    private static final String NULL_VALUE = "{null}";
    private static final Property<String> PROPERTY_REST_IP = Property.named("service.restIp", "0.0.0.0");
    private static final Property<Integer> PROPERTY_REST_PORT = Property.named("service.restPort", 9092);
    
    private static final Property<String> PROPERTY_PRAVEGA_CONTROLLER_URL = Property.named("service.controller.url", "tcp://localhost:9090");
    private static final Property<String> PROPERTY_STORE_TYPE = Property.named("service.storeType", "Pravega");
    private static final Property<Integer> PROPERTY_THREAD_POOL_SIZE = Property.named("service.threadPoolSize", 50);

    private static final Property<Boolean> PROPERTY_TLS_ENABLED = Property.named("auth.tlsEnabled", false);
    private static final Property<String> PROPERTY_TLS_CERT_FILE = Property.named("auth.tlsCertFile", "");
    private static final Property<String> PROPERTY_TLS_TRUST_STORE = Property.named("auth.tlsTrustStore", "");
    private static final Property<String> PROPERTY_TLS_KEY_FILE = Property.named("auth.tlsKeyFile", "");
    private static final Property<String> PROPERTY_TLS_KEY_PASSWORD_FILE = Property.named("auth.tlsKeyPasswordFile", "");

    private static final Property<Boolean> PROPERTY_AUTHORIZATION_ENABLED = Property.named("auth.enabled", false);
    private static final Property<String> PROPERTY_AUTHORIZATION_PASSWORD_FILE = Property.named("auth.userPasswordFile", "");

    private static final String COMPONENT_CODE = "schema-registry";

    //endregion

    //region Initialization

    static {
        val properties = loadConfiguration();
        val p = new TypedProperties(properties, COMPONENT_CODE);

        SERVICE_HOST = p.get(PROPERTY_REST_IP);
        SERVICE_PORT = p.getInt(PROPERTY_REST_PORT);

        PRAVEGA_CONTROLLER_URI = p.get(PROPERTY_PRAVEGA_CONTROLLER_URL);

        THREAD_POOL_SIZE = p.getInt(PROPERTY_THREAD_POOL_SIZE);
        STORE_TYPE = p.get(PROPERTY_STORE_TYPE);

        TLS_ENABLED = p.getBoolean(PROPERTY_TLS_ENABLED);
        TLS_KEY_FILE = p.get(PROPERTY_TLS_KEY_FILE);
        TLS_KEY_PASSWORD_FILE = p.get(PROPERTY_TLS_KEY_PASSWORD_FILE);
        TLS_CERT_FILE = p.get(PROPERTY_TLS_CERT_FILE);
        TLS_TRUST_STORE = p.get(PROPERTY_TLS_TRUST_STORE);

        AUTHORIZATION_ENABLED = p.getBoolean(PROPERTY_AUTHORIZATION_ENABLED);
        USER_PASSWORD_FILE = p.get(PROPERTY_AUTHORIZATION_PASSWORD_FILE);

        SERVICE_CONFIG = createServiceConfig();
    }

    private static Properties loadConfiguration() {
        // Fetch configuration in a specific order (from lowest priority to highest).
        Properties properties = new Properties();
        properties.putAll(loadFromFile());
        properties.putAll(System.getenv());
        properties.putAll(System.getProperties());

        // Resolve references against the loaded properties.
        properties = resolveReferences(properties);

        log.info("Registry configuration:");
        properties.forEach((k, v) -> log.info("{} = {}", k, v));
        return properties;
    }

    @SneakyThrows(IOException.class)
    private static Properties loadFromFile() {
        Properties result = new Properties();

        File file = findConfigFile();
        if (file == null) {
            ClassLoader classLoader = Config.class.getClassLoader();
            URL url = classLoader.getResource("schema-registry.config.properties");
            if (url != null) {
                file = new File(url.getFile());
                if (!file.exists()) {
                    file = null;
                }
            }
        }

        if (file != null) {
            try (FileReader reader = new FileReader(file)) {
                result.load(reader);
            }
            log.info("Loaded {} config properties from {}.", result.size(), file);
        }

        return result;
    }

    private static File findConfigFile() {
        File result = Arrays.stream(new String[]{"conf.file", "config.file"})
                            .map(System::getProperty)
                            .filter(s -> !Strings.isNullOrEmpty(s))
                            .map(File::new)
                            .filter(File::exists)
                            .findFirst()
                            .orElse(new File("schema-registry.config.properties"));

        return result.exists() ? result : null;
    }

    private static Properties resolveReferences(Properties properties) {
        // Any value that looks like ${REF} will need to be replaced by the value of REF in source.
        final String pattern = "^\\$\\{(.+)\\}$";
        val resolved = new Properties();
        for (val e : properties.entrySet()) {
            // Fetch reference value.
            String existingValue = e.getValue().toString();
            String newValue = existingValue; // Default to existing value (in case it's not a reference).
            if (existingValue.matches(pattern)) {
                // Only include the referred value if it resolves to anything; otherwise exclude this altogether.
                String lookupKey = existingValue.replaceAll(pattern, "$1");
                newValue = (String) properties.getOrDefault(lookupKey, null);
                if (newValue != null) {
                    log.info("Config property '{}={}' resolved to '{}'.", e.getKey(), existingValue, newValue);
                }
            }

            if (newValue != null) {
                resolved.put(e.getKey().toString(), newValue);
            }
        }

        return resolved;
    }

    private static ServiceConfig createServiceConfig() {
        return ServiceConfig.builder()
                            .host(Config.SERVICE_HOST)
                                   .port(Config.SERVICE_PORT)
                                   .authEnabled(Config.AUTHORIZATION_ENABLED)
                                   .userPasswordFile(Config.USER_PASSWORD_FILE)
                                   .tlsEnabled(Config.TLS_ENABLED)
                                   .tlsCertFile(Config.TLS_CERT_FILE)
                                   .tlsKeyFilePath(Config.TLS_KEY_FILE)
                                   .tlsKeyFilePath(Config.TLS_KEY_PASSWORD_FILE)
                                   .tlsTrustStore(Config.TLS_TRUST_STORE)
                                   .build();
    }

    //endregion
}

