/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.samples;

import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.service.SchemaRegistryService;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PassthruSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryService service;

    public PassthruSchemaRegistryClient(SchemaRegistryService service) {
        this.service = service;
    }
    
    @Override
    public boolean addGroup(String group, GroupProperties properties) {
        return service.createGroup(group, properties).join();
    }

    @Override
    public void removeGroup(String group) {
        service.deleteGroup(group).join();
    }

    @Override
    public Map<String, GroupProperties> listGroups() {
        return service.listGroups(null, 100).thenApply(MapWithToken::getMap).join();
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        return service.getGroupProperties(group).join();
    }

    @Override
    public void updateSchemaValidationRules(String group, SchemaValidationRules validationRules, SchemaValidationRules previousRules) {
        service.updateSchemaValidationRules(group, validationRules, previousRules).join();
    }
    
    @Override
    public List<SchemaWithVersion> getSchemas(String group) {
        return service.getSchemas(group, null).join();
    }

    @Override
    public VersionInfo addSchema(String group, SchemaInfo schema) {
        return service.addSchema(group, schema).join();
    }

    @Override
    public void deleteSchemaVersion(String groupId, VersionInfo version) {
        service.deleteSchema(groupId, version.getOrdinal()).join();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String group, VersionInfo version) {
        return service.getSchema(group, version.getOrdinal()).join();
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        return service.getEncodingInfo(group, encodingId).join();
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo version, String codecType) {
        return service.getEncodingId(group, version, codecType).join();
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String group, @Nullable String schemaType) {
        List<SchemaWithVersion> latestSchemas = service.getSchemas(group, schemaType).join();
        if (schemaType == null) {
            return latestSchemas.stream().max(Comparator.comparingInt(x -> x.getVersion().getOrdinal())).orElse(null);
        } else {
            return latestSchemas.get(0);
        }
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String groupId, @Nullable String schemaType) {
        return service.getGroupHistory(groupId, schemaType)
                .thenApply(history -> history.stream().map(x -> new SchemaWithVersion(x.getSchema(), x.getVersion())).collect(Collectors.toList())).join();
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String group) {
        return service.getGroupHistory(group, null).join();
    }

    @Override
    public Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException {
        return service.getSchemaReferences(schemaInfo).join();
    }

    @Override
    public VersionInfo getVersionForSchema(String group, SchemaInfo schema) {
        return service.getSchemaVersion(group, schema).join();
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schema) {
        return service.validateSchema(group, schema).join();
    }

    @Override
    public boolean canReadUsing(String group, SchemaInfo schema) {
        return service.canRead(group, schema).join();
    }

    @Override
    public List<String> getCodecTypes(String group) {
        return service.getCodecTypes(group).join();
    }

    @Override
    public void addCodecType(String group, String codecType) {
        service.addCodecType(group, codecType).join();
    }
}
