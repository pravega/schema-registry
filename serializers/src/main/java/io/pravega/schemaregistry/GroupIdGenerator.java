/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry;

import lombok.SneakyThrows;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public interface GroupIdGenerator {

    @SneakyThrows
    static String getGroupId(Type type, String... args) {
        switch (type) {
            case QualifiedStreamName:
                StringBuilder qualifiedNameBuilder = new StringBuilder();
                for (String arg : args) {
                    qualifiedNameBuilder.append(arg);
                    qualifiedNameBuilder.append("/");
                }
                return URLEncoder.encode(qualifiedNameBuilder.toString(), StandardCharsets.UTF_8.toString());
            default:
                throw new IllegalArgumentException();
        }
    }
    
    enum Type {
        QualifiedStreamName,
    }
}
