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
import io.pravega.schemaregistry.contract.data.Compatibility;
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
    private final String namespace;

    public PassthruSchemaRegistryClient(SchemaRegistryService service, String namespace) {
        this.service = service;
        this.namespace = namespace;
    }

    public PassthruSchemaRegistryClient(SchemaRegistryService service) {
        this(service, null);
    }

    @Override
    public boolean addGroup(String group, GroupProperties properties) {
        return service.createGroup(namespace, group, properties).join();
    }

    @Override
    public void removeGroup(String group) {
        service.deleteGroup(namespace, group).join();
    }

    @Override
    public Iterator<Map.Entry<String, GroupProperties>> listGroups() {
        Function<ContinuationToken, Map.Entry<ContinuationToken, Collection<Map.Entry<String, GroupProperties>>>> function =
                token -> {
                    MapWithToken<String, GroupProperties> result = service.listGroups(namespace, token, 100).join();
                    return new AbstractMap.SimpleEntry<>(result.getToken(), result.getMap().entrySet());
                };
        return new ContinuationTokenIterator<>(function, null);
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        return service.getGroupProperties(namespace, group).join();
    }

    @Override
    public boolean updateCompatibility(String group, Compatibility validationRules, Compatibility previousRules) {
        return service.updateCompatibility(namespace, group, validationRules, previousRules)
                      .handle((r, e) -> e != null).join();
    }

    @Override
    public List<SchemaWithVersion> getSchemas(String group) {
        return service.getSchemas(namespace, group, null).join();
    }

    @Override
    public VersionInfo addSchema(String group, SchemaInfo schemaInfo) {
        return service.addSchema(namespace, group, schemaInfo).join();
    }

    @Override
    public void deleteSchemaVersion(String group, VersionInfo versionInfo) {
        service.deleteSchema(namespace, group, versionInfo.getId()).join();
    }

    @Override
    public void deleteSchemaVersion(String group, String schemaType, int version) {
        service.deleteSchema(group, schemaType, version).join();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String group, VersionInfo versionInfo) {
        return service.getSchema(namespace, group, versionInfo.getId()).join();
    }

    @Override
    public SchemaInfo getSchemaForVersion(String group, String schemaType, int version) {
        return service.getSchema(namespace, group, schemaType, version).join();
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        return service.getEncodingInfo(namespace, group, encodingId).join();
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo versionInfo, String codecType) {
        return service.getEncodingId(namespace, group, versionInfo, codecType).join();
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String group, @Nullable String schemaType) {
        List<SchemaWithVersion> latestSchemas = service.getSchemas(namespace, group, schemaType).join();
        if (schemaType == null) {
            return latestSchemas.stream().max(Comparator.comparingInt(x -> x.getVersionInfo().getId())).orElse(null);
        } else {
            return latestSchemas.get(0);
        }
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String group, @Nullable String schemaType) {
        return service.getGroupHistory(namespace, group, schemaType)
                      .thenApply(history -> history.stream().map(x -> new SchemaWithVersion(x.getSchema(), x.getVersion())).collect(Collectors.toList())).join();
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String group) {
        return service.getGroupHistory(namespace, group, null).join();
    }

    @Override
    public Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws RegistryExceptions.ResourceNotFoundException, RegistryExceptions.UnauthorizedException {
        return service.getSchemaReferences(namespace, schemaInfo).join();
    }

    @Override
    public VersionInfo getVersionForSchema(String group, SchemaInfo schemaInfo) {
        return service.getSchemaVersion(namespace, group, schemaInfo).join();
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schemaInfo) {
        return service.validateSchema(namespace, group, schemaInfo).join();
    }

    @Override
    public boolean canReadUsing(String group, SchemaInfo schemaInfo) {
        return service.canRead(namespace, group, schemaInfo).join();
    }

    @Override
    public List<String> getCodecTypes(String group) {
        return service.getCodecTypes(namespace, group).join();
    }

    @Override
    public void addCodecType(String group, String codecType) {
        service.addCodecType(namespace, group, codecType).join();
    }
}
