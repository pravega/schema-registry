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
import io.pravega.schemaregistry.common.ContinuationTokenIterator;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
    public Iterator<Map.Entry<String, GroupProperties>> listGroups() {
        Function<ContinuationToken, Map.Entry<ContinuationToken, Collection<Map.Entry<String, GroupProperties>>>> function = 
                token -> {
                    MapWithToken<String, GroupProperties> result = service.listGroups(token, 100).join();
                    return new AbstractMap.SimpleEntry<>(result.getToken(), result.getMap().entrySet());
                }; 
        return new ContinuationTokenIterator<>(function, null);
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        return service.getGroupProperties(group).join();
    }

    @Override
    public boolean updateSchemaValidationRules(String group, SchemaValidationRules validationRules, SchemaValidationRules previousRules) {
        return service.updateSchemaValidationRules(group, validationRules, previousRules)
                      .handle((r, e) -> e != null).join();
    }
    
    @Override
    public List<SchemaWithVersion> getSchemas(String group) {
        return service.getSchemas(group, null).join();
    }

    @Override
    public VersionInfo addSchema(String group, SchemaInfo schemaInfo) {
        return service.addSchema(group, schemaInfo).join();
    }

    @Override
    public void deleteSchemaVersion(String groupId, VersionInfo versionInfo) {
        service.deleteSchema(groupId, versionInfo.getOrdinal()).join();
    }

    @Override
    public void deleteSchemaVersion(String groupId, String schemaType, int version) {
        service.deleteSchema(groupId, schemaType, version).join();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String group, VersionInfo versionInfo) {
        return service.getSchema(group, versionInfo.getOrdinal()).join();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String group, String schemaType, int version) {
        return service.getSchema(group, schemaType, version).join();
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        return service.getEncodingInfo(group, encodingId).join();
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo versionInfo, String codecType) {
        return service.getEncodingId(group, versionInfo, codecType).join();
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String group, @Nullable String schemaType) {
        List<SchemaWithVersion> latestSchemas = service.getSchemas(group, schemaType).join();
        if (schemaType == null) {
            return latestSchemas.stream().max(Comparator.comparingInt(x -> x.getVersionInfo().getOrdinal())).orElse(null);
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
    public VersionInfo getVersionForSchema(String group, SchemaInfo schemaInfo) {
        return service.getSchemaVersion(group, schemaInfo).join();
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schemaInfo) {
        return service.validateSchema(group, schemaInfo).join();
    }

    @Override
    public boolean canReadUsing(String group, SchemaInfo schemaInfo) {
        return service.canRead(group, schemaInfo).join();
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
