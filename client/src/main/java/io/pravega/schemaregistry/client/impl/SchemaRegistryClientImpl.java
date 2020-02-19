/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolutionEpoch;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateScopeRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaTypeModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRulesModel;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import org.glassfish.jersey.client.ClientConfig;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;

public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private final Client client = ClientBuilder.newClient(new ClientConfig());
    private final URI uri;

    public SchemaRegistryClientImpl(URI uri) {
        this.uri = uri;
    }
    
    @Override
    public void createScope(String scope) {
        WebTarget webTarget = client.target(uri).path("scopes");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        CreateScopeRequest request = new CreateScopeRequest().scopeName(scope);
        invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteScope(String scope) {
        WebTarget webTarget = client.target(uri).path("scopes").path(scope);

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        CreateScopeRequest request = new CreateScopeRequest().scopeName(scope);
        invocationBuilder.delete();
    }

    @Override
    public boolean addGroup(String scope, String group, SchemaType schemaType, SchemaValidationRules validationRules, boolean subgroupBySchemaName, boolean enableEncoding) {
        WebTarget webTarget = client.target(uri).path("scopes").path(scope).path("groups");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        SchemaTypeModel schemaTypeModel = ModelHelper.encode(schemaType);

        SchemaValidationRulesModel compatibility = ModelHelper.encode(validationRules);
        CreateGroupRequest request = new CreateGroupRequest().schemaType(schemaTypeModel)
                                                             .enableEncoding(enableEncoding).groupByEventType(subgroupBySchemaName)
                                                             .groupName(group)
                                                             .validationRules(compatibility);
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return true;
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            return false;
        } else {
            throw new RuntimeException("Failed");
        }
    }

    @Override
    public void removeGroup(String scope, String group) {

    }

    @Override
    public GroupProperties getGroupProperties(String scope, String group) {
        return null;
    }

    @Override
    public void updateSchemaValidationRules(String scope, String group, SchemaValidationRules validationRules) {

    }

    @Override
    public List<String> getSubgroups(String scope, String group) {
        return null;
    }

    @Override
    public VersionInfo addSchemaIfAbsent(String scope, String group, SchemaInfo schema, SchemaValidationRules rules) {
        return null;
    }

    @Override
    public SchemaInfo getSchema(String scope, String group, VersionInfo version) {
        return null;
    }

    @Override
    public EncodingInfo getEncodingInfo(String scope, String group, EncodingId encodingId) {
        return null;
    }

    @Override
    public EncodingId getEncodingId(String scope, String group, VersionInfo version, CompressionType compressionType) {
        return null;
    }

    @Override
    public SchemaWithVersion getLatestSchema(String scope, String group, @Nullable String subgroup) {
        return null;
    }

    @Override
    public List<SchemaEvolutionEpoch> getGroupEvolutionHistory(String scope, String group, @Nullable String subgroup) {
        return null;
    }

    @Override
    public VersionInfo getSchemaVersion(String scope, String group, SchemaInfo schema) {
        return null;
    }

    @Override
    public boolean validateSchema(String scope, String group, SchemaInfo schema, SchemaValidationRules validationRules) {
        return false;
    }

    @Override
    public List<CompressionType> getCompressions(String scope, String group) {
        return null;
    }
}
