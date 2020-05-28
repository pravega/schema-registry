/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.serde;

import io.pravega.schemaregistry.serializers.PravegaDeserializer;
import io.pravega.schemaregistry.serializers.PravegaSerializer;
import lombok.SneakyThrows;

import java.net.URL;
import java.security.PrivilegedAction;

import static java.security.AccessController.doPrivileged;

public class SerdeLoader {
    public static PravegaSerializer getSerializer(String className, URL url) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final PrivilegedAction<PravegaSerializer> action = new PrivilegedAction<PravegaSerializer>() {
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public PravegaSerializer run() {
                    Class<?> aClass;
                    // Load the class.
                    URL[] urls = {url};
                    aClass = Class.forName(className, true, new java.net.URLClassLoader(urls));
                if (PravegaSerializer.class.isAssignableFrom(aClass)) {
                    return (PravegaSerializer) aClass.newInstance();
                } else {
                    throw new RuntimeException("implementation not found");
                }
            }
        };
        return doPrivileged(action);
    }

    @SuppressWarnings("unchecked")
    public static PravegaDeserializer getDeserializer(String className, URL url) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final PrivilegedAction<PravegaDeserializer> action = new PrivilegedAction<PravegaDeserializer>() {
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public PravegaDeserializer run() {
                Class<?> theClass;
                // Load the class.
                URL[] urls = {url};
                theClass = Class.forName(className, true, new java.net.URLClassLoader(urls));
                if (PravegaDeserializer.class.isAssignableFrom(theClass)) {
                    // Create an instance of the class.
                    return (PravegaDeserializer) theClass.newInstance();
                } else {
                    throw new RuntimeException("Implementation not found");
                }
            }
        };
        return doPrivileged(action);
    }
}
