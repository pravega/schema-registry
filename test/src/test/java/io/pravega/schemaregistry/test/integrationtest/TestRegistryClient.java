/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.test.integrationtest;

import io.pravega.schemaregistry.client.impl.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.service.SchemaRegistryService;

import javax.annotation.Nullable;
import java.util.List;

public class TestRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryService service;

    public TestRegistryClient(SchemaRegistryService service) {
        this.service = service;
    }

    @Override
    public void createNamespace(String namespace) {
        service.createNamespace(namespace).join();
    }

    @Override
    public void deleteNamespace(String namespace) {
        service.deleteNamespace(namespace).join();
    }

    @Override
    public boolean addGroup(String namespace, String group, SchemaType schemaType, SchemaValidationRules validationRules, 
                            boolean subgroupBySchemaName, boolean enableEncoding) {
        return service.createGroup(namespace, group, new GroupProperties(schemaType, validationRules, subgroupBySchemaName, enableEncoding)).join();
    }

    @Override
    public void removeGroup(String namespace, String group) {
        service.deleteGroup(namespace, group).join();
    }

    @Override
    public GroupProperties getGroupProperties(String namespace, String group) {
        return service.getGroupProperties(namespace, group).join();
    }

    @Override
    public void updateSchemaValidationRules(String namespace, String group, SchemaValidationRules validationRules) {
        service.updateSchemaValidationPolicy(namespace, group, validationRules).join();
    }

    @Override
    public List<String> getSubgroups(String namespace, String group) {
        return service.getSubgroups(namespace, group, null).join().getList();
    }

    @Override
    public VersionInfo addSchemaIfAbsent(String namespace, String group, SchemaInfo schema, SchemaValidationRules rules) {
        return service.addSchemaIfAbsent(namespace, group, schema, rules).join();
    }

    @Override
    public SchemaInfo getSchema(String namespace, String group, VersionInfo version) {
        return service.getSchema(namespace, group, version).join();
    }

    @Override
    public EncodingInfo getEncodingInfo(String namespace, String group, EncodingId encodingId) {
        return service.getEncodingInfo(namespace, group, encodingId).join();
    }

    @Override
    public EncodingId getEncodingId(String namespace, String group, VersionInfo version, CompressionType compressionType) {
        return service.getEncodingId(namespace, group, version, compressionType).join();
    }

    @Override
    public SchemaWithVersion getLatestSchema(String namespace, String group, @Nullable String subgroup) {
        return service.getLatestSchema(namespace, group, subgroup).join();
    }

    @Override
    public List<SchemaEvolution> getGroupEvolutionHistory(String namespace, String group, @Nullable String subgroup) {
        return service.getGroupEvolutionHistory(namespace, group, subgroup).join();
    }

    @Override
    public VersionInfo getSchemaVersion(String namespace, String group, SchemaInfo schema) {
        return service.getSchemaVersion(namespace, group, schema).join();
    }

    @Override
    public boolean validateSchema(String namespace, String group, SchemaInfo schema, SchemaValidationRules validationRules) {
        return service.validateSchema(namespace, group, schema, validationRules).join();
    }

    @Override
    public List<CompressionType> getCompressions(String namespace, String group) {
        return service.getCompressions(namespace, group).join();
    }
}
