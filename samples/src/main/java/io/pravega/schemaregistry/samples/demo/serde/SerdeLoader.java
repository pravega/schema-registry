/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples.demo.serde;

import io.pravega.schemaregistry.serializer.shared.impl.CustomDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.CustomSerializer;
import lombok.SneakyThrows;

import java.net.URL;
import java.security.PrivilegedAction;

import static java.security.AccessController.doPrivileged;

public class SerdeLoader {
    public static CustomSerializer getSerializer(String className, URL url) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final PrivilegedAction<CustomSerializer> action = new PrivilegedAction<CustomSerializer>() {
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public CustomSerializer run() {
                    Class<?> aClass;
                    // Load the class.
                    URL[] urls = {url};
                    aClass = Class.forName(className, true, new java.net.URLClassLoader(urls));
                if (CustomSerializer.class.isAssignableFrom(aClass)) {
                    return (CustomSerializer) aClass.getDeclaredConstructor().newInstance();
                } else {
                    throw new RuntimeException("implementation not found");
                }
            }
        };
        return doPrivileged(action);
    }

    @SuppressWarnings("unchecked")
    public static CustomDeserializer getDeserializer(String className, URL url) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final PrivilegedAction<CustomDeserializer> action = new PrivilegedAction<CustomDeserializer>() {
            @Override
            @SneakyThrows
            @SuppressWarnings("unchecked")
            public CustomDeserializer run() {
                Class<?> theClass;
                // Load the class.
                URL[] urls = {url};
                theClass = Class.forName(className, true, new java.net.URLClassLoader(urls));
                if (CustomDeserializer.class.isAssignableFrom(theClass)) {
                    // Create an instance of the class.
                    return (CustomDeserializer) theClass.getDeclaredConstructor().newInstance();
                } else {
                    throw new RuntimeException("Implementation not found");
                }
            }
        };
        return doPrivileged(action);
    }
}
