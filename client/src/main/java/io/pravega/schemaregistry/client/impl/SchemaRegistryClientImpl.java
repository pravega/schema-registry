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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.ObjectTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import org.apache.commons.lang3.NotImplementedException;
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
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private static final HashFunction HASH = Hashing.murmur3_128();
    private final Client client = ClientBuilder.newClient(new ClientConfig());
    private final URI uri;

    public SchemaRegistryClientImpl(URI uri) {
        this.uri = uri;
    }
    
    @Override
    public boolean addGroup(String group, SchemaType schemaType, SchemaValidationRules validationRules, boolean validateByObjectType, boolean enableEncoding) {
        WebTarget webTarget = client.target(uri).path("v1/groups");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaType schemaTypeModel = ModelHelper.encode(schemaType);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules compatibility = ModelHelper.encode(validationRules);
        CreateGroupRequest request = new CreateGroupRequest().schemaType(schemaTypeModel)
                                                             .enableEncoding(enableEncoding).validateByObjectType(validateByObjectType)
                                                             .groupName(group)
                                                             .validationRules(compatibility);
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return true;
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            return false;
        } else {
            throw new RuntimeException("Internal Service error. Failed to add the group.");
        }
    }

    @Override
    public void removeGroup(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.delete();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to remove the group.");
        }
    }

    @Override
    public Map<String, GroupProperties> listGroups() {
        WebTarget webTarget = client.target(uri).path("v1/groups");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        Response response = invocationBuilder.get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to list groups.");
        }

        GroupsList entity = response.readEntity(GroupsList.class);
        return entity.getGroups().stream().collect(Collectors.toMap(x -> x.getGroupName(), 
                x -> {
                    SchemaType schemaType = ModelHelper.decode(x.getSchemaType());
                    SchemaValidationRules rules = ModelHelper.decode(x.getSchemaValidationRules());
                    return new GroupProperties(schemaType, rules, x.isValidateByObjectType(), x.isEnableEncoding());
                }));
    }

    @Override
    public GroupProperties getGroupProperties(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Group not found.");
        } else {
            throw new RuntimeException("Internal error. Failed to get group properties.");
        }
    }

    @Override
    public void updateSchemaValidationRules(String group, SchemaValidationRules validationRules) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group);
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        UpdateValidationRulesPolicyRequest request = new UpdateValidationRulesPolicyRequest();
        request.setValidationRules(ModelHelper.encode(validationRules));

        Response response = invocationBuilder.put(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new RuntimeException("Conflict attempting to update the rules. Try again.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to update schema validation rules.");
        }
    }

    @Override
    public void addSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("Rule not implemented");
    }

    @Override
    public void removeSchemaValidationRule(String group, SchemaValidationRule rule) {
        throw new NotImplementedException("Rule not implemented");
    }

    @Override
    public List<String> getObjectTypes(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("objectTypes");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        ObjectTypesList objectTypesList = response.readEntity(ObjectTypesList.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return objectTypesList.getGroups();
        } else {
            throw new RuntimeException("Internal Service error. Failed to get objectTypes.");
        }    
    }

    @Override
    public VersionInfo addSchemaIfAbsent(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        AddSchemaToGroupRequest addSchemaToGroupRequest = new AddSchemaToGroupRequest();
        addSchemaToGroupRequest.schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Group not found.");
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new RuntimeException("Schema is incompatible.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get objectTypes.");
        }
    }

    @Override
    public SchemaInfo getSchema(String group, VersionInfo version) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions").path(Integer.toString(version.getVersion()));
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get objectTypes.");
        }
    }

    @Override
    public EncodingInfo getEncodingInfo(String group, EncodingId encodingId) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("encodings").path(Integer.toString(encodingId.getId()));
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Encoding not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public EncodingId getEncodingId(String group, VersionInfo version, CompressionType compressionType) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("encodings");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest();
        getEncodingIdRequest.compressionType(ModelHelper.encode(compressionType))
                            .versionInfo(ModelHelper.encode(version));
        Response response = invocationBuilder.put(Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEncodingId failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public SchemaWithVersion getLatestSchema(String group, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getLatestSchemaForGroup(group);
        } else {
            return getLatestSchemaByObjectType(group, objectTypeName);
        }
    }

    private SchemaWithVersion getLatestSchemaForGroup(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions").path("latest");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    private SchemaWithVersion getLatestSchemaByObjectType(String group, String objectTypeName) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group)
                                    .path("objectTypes").path(objectTypeName).path("schemas").path("versions").path("latest");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    private SchemaWithVersion processLatestSchemaResponse(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class));
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public List<SchemaEvolution> getGroupEvolutionHistory(String group, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getEvolutionHistory(group);
        } else {
            return getEvolutionHistoryByObjectType(group, objectTypeName);
        }
    }
    
    private List<SchemaEvolution> getEvolutionHistory(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("versions");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEvolutionHistory failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    private List<SchemaEvolution> getEvolutionHistoryByObjectType(String group, String objectTypeName) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("objectTypes").path(objectTypeName)
                                    .path("schemas").path("versions");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("getEvolutionHistory failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    private List<SchemaEvolution> processHistoryResponse(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaList schemaList = response.readEntity(SchemaList.class);
            return schemaList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else {
            throw new RuntimeException("Internal Service error. Failed to get group history.");
        }
    }

    @Override
    public VersionInfo getSchemaVersion(String group, SchemaInfo schema) {
        long fingerprint = HASH.hashBytes(schema.getSchemaData()).asLong();

        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas/schema").path(Long.toString(fingerprint));

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schema));

        Response response = invocationBuilder.post(Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema version.");
        }
    }

    @Override
    public boolean validateSchema(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("validate");
        
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        ValidateRequest validateRequest = new ValidateRequest()
                .schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(validateRequest, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Valid.class).isValid();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @Override
    public boolean canRead(String group, SchemaInfo schema) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("schemas").path("canRead");

        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        CanReadRequest request = new CanReadRequest().schemaInfo(ModelHelper.encode(schema));
        Response response = invocationBuilder.post(Entity.entity(request, MediaType.APPLICATION_JSON));
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(CanRead.class).isCompatible();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new RuntimeException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }
    
    @Override
    public List<CompressionType> getCompressions(String group) {
        WebTarget webTarget = client.target(uri).path("v1/groups").path(group).path("compressions");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        Response response = invocationBuilder.get();
        CompressionsList list = response.readEntity(CompressionsList.class);
        
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return list.getCompressionTypes().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else {
            throw new RuntimeException("Failed to get compressions");
        }    
    }
}
