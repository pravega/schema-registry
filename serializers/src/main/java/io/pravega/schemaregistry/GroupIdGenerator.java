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

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Defines strategies for generating groupId for stream. 
 * Currently there is only one naming strategy that uses streams fully qualified scoped stream name and encodes it using
 * URL encoder.
 */
public class GroupIdGenerator {
    private GroupIdGenerator() {
    }

    @SneakyThrows
    public static String getGroupId(Type type, String... args) {
        switch (type) {
            case QualifiedStreamName:
                Preconditions.checkNotNull(args);
                Preconditions.checkArgument(args.length == 2);
                StringBuilder qualifiedNameBuilder = new StringBuilder();
                qualifiedNameBuilder.append("pravega://");
                for (String arg : args) {
                    qualifiedNameBuilder.append(arg);
                    qualifiedNameBuilder.append("/");
                }
                return URLEncoder.encode(qualifiedNameBuilder.toString(), StandardCharsets.UTF_8.toString());
            default:
                throw new IllegalArgumentException();
        }
    }
    
    public enum Type {
        QualifiedStreamName,
    }
}
