/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializer.shared.impl;

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.serializer.shared.credentials.PravegaCredentialProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SerializerFactoryHelper {
    public static SchemaRegistryClient initForSerializer(SerializerConfig config) {
        SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(config);
        createGroup(schemaRegistryClient, config);
        registerCodec(schemaRegistryClient, config);
        return schemaRegistryClient;
    }

    public static SchemaRegistryClient initForDeserializer(SerializerConfig config) {
        SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(config);
        createGroup(schemaRegistryClient, config);
        failOnCodecMismatch(schemaRegistryClient, config);
        return schemaRegistryClient;
    }

    private static SchemaRegistryClient getSchemaRegistryClient(SerializerConfig config) {
        if (config.getRegistryConfigOrClient().isLeft()) {
            // if auth is enabled and creds are not supplied, reuse the credentials from pravega client config which may
            // be loaded from system properties. 
            SchemaRegistryClientConfig left = config.getRegistryConfigOrClient().getLeft();
            if (left.isAuthEnabled() && left.getCredentialProvider() == null) {
                left = SchemaRegistryClientConfig.builder().schemaRegistryUri(left.getSchemaRegistryUri())
                                                 .credentialProvider(new PravegaCredentialProvider())
                                                 .build();
            }
            return SchemaRegistryClientFactory.withNamespace(config.getNamespace(), left);
        } else {
            return config.getRegistryConfigOrClient().getRight();
        }
    }

    private static void createGroup(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isCreateGroup()) {
            client.addGroup(config.getGroupId(), config.getGroupProperties());
        }
    }

    private static void registerCodec(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isRegisterCodec()) {
            client.addCodecType(config.getGroupId(), config.getEncoder().getCodecType());
        }
    }

    private static void failOnCodecMismatch(SchemaRegistryClient client, SerializerConfig config) {
        if (config.isFailOnCodecMismatch()) {
            List<String> codecTypesInGroup = client.getCodecTypes(config.getGroupId()).stream()
                                                   .map(CodecType::getName).collect(Collectors.toList());
            if (!config.getDecoders().getDecoderNames().containsAll(codecTypesInGroup)) {
                log.warn("Not all CodecTypes are supported by reader. Required codecTypes = {}", codecTypesInGroup);
                throw new RuntimeException(String.format("Need all codecTypes in %s", codecTypesInGroup.toString()));
            }
        }
    }
}
