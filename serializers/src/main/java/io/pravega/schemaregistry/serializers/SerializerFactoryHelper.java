/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
class SerializerFactoryHelper {
    static void autoCreateGroup(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isCreateGroup()) {
            client.addGroup(config.getGroupId(), config.getGroupProperties());
        }
    }

    static void registerCodec(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isRegisterCodec()) {
            client.addCodecType(config.getGroupId(), config.getCodec().getCodecType());
        }
    }

    static void failOnCodecMismatch(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isFailOnCodecMismatch()) {
            List<String> codecTypesInGroup = client.getCodecTypes(config.getGroupId());
            if (!config.getDecoder().getCodecTypes().containsAll(codecTypesInGroup)) {
                log.warn("Not all CodecTypes are supported by reader. Required codecTypes = {}", codecTypesInGroup);
                throw new RuntimeException(String.format("Need all codecTypes in %s", codecTypesInGroup.toString()));
            }
        }
    }
}
