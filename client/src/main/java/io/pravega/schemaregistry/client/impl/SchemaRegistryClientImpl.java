/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.NotFoundException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
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
import io.pravega.schemaregistry.contract.v1.ApiV1;
import lombok.SneakyThrows;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.proxy.WebResourceFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private final ApiV1.GroupsApi proxy;

    public SchemaRegistryClientImpl(URI uri) {
        Client client = ClientBuilder.newClient(new ClientConfig());
        this.proxy = WebResourceFactory.newResource(ApiV1.GroupsApi.class, client.target(uri));
    }

    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi proxy) {
        this.proxy = proxy;
    }

    @SneakyThrows
    @Override
    public boolean addGroup(String groupId, SchemaType schemaType, SchemaValidationRules validationRules, boolean validateByObjectType, Map<String, String> properties) {
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaType schemaTypeModel = ModelHelper.encode(schemaType);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules compatibility = ModelHelper.encode(validationRules);
        CreateGroupRequest request = new CreateGroupRequest().schemaType(schemaTypeModel)
                                                             .properties(properties).validateByObjectType(validateByObjectType)
                                                             .groupName(groupId)
                                                             .validationRules(compatibility);

        Response response = proxy.createGroup(request);
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            return true;
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            return false;
        } else {
            throw new RuntimeException("Internal Service error. Failed to add the group.");
        }
    }

    @SneakyThrows
    @Override
    public void removeGroup(String groupId) {
        Response response = proxy.deleteGroup(groupId);
        if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to remove the group.");
        }
    }

    @SneakyThrows
    @Override
    public Map<String, GroupProperties> listGroups() {
        Response response = proxy.listGroups();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to list groups.");
        }

        GroupsList entity = response.readEntity(GroupsList.class);
        return entity.getGroups().stream().collect(Collectors.toMap(x -> x.getGroupName(),
                x -> {
                    SchemaType schemaType = ModelHelper.decode(x.getSchemaType());
                    SchemaValidationRules rules = ModelHelper.decode(x.getSchemaValidationRules());
                    return new GroupProperties(schemaType, rules, x.isValidateByObjectType(), x.getProperties());
                }));
    }

    @SneakyThrows
    @Override
    public GroupProperties getGroupProperties(String groupId) {
        Response response = proxy.getGroupProperties(groupId);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal error. Failed to get group properties.");
        }
    }

    @SneakyThrows
    @Override
    public void updateSchemaValidationRules(String groupId, SchemaValidationRules validationRules) {
        UpdateValidationRulesPolicyRequest request = new UpdateValidationRulesPolicyRequest()
                .validationRules(ModelHelper.encode(validationRules));

        Response response = proxy.updateSchemaValidationRules(groupId, request);
        if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new PreconditionFailedException("Conflict attempting to update the rules. Try again.");
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to update schema validation rules.");
        }
    }

    @SneakyThrows
    @Override
    public List<String> getObjectTypes(String groupId) {
        Response response = proxy.getObjectTypes(groupId);
        ObjectTypesList objectTypesList = response.readEntity(ObjectTypesList.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return objectTypesList.getObjectTypes();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get objectTypes.");
        }
    }

    @SneakyThrows
    @Override
    public VersionInfo addSchema(String groupId, SchemaInfo schema) {
        AddSchemaToGroupRequest addSchemaToGroupRequest = new AddSchemaToGroupRequest();
        addSchemaToGroupRequest.schemaInfo(ModelHelper.encode(schema));
        Response response = proxy.addSchemaToGroupIfAbsent(groupId, addSchemaToGroupRequest);
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new IncompatibleSchemaException("Schema is incompatible.");
        } else if (response.getStatus() == Response.Status.EXPECTATION_FAILED.getStatusCode()) {
            throw new SchemaTypeMismatchException("Schema type is invalid.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to addSchema.");
        }
    }

    @SneakyThrows
    @Override
    public SchemaInfo getSchema(String groupId, VersionInfo version) {
        Response response = proxy.getSchemaFromVersion(groupId, version.getOrdinal());
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema.");
        }
    }

    @SneakyThrows
    @Override
    public EncodingInfo getEncodingInfo(String groupId, EncodingId encodingId) {
        Response response = proxy.getEncodingInfo(groupId, encodingId.getId());
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Encoding not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @SneakyThrows
    @Override
    public EncodingId getEncodingId(String groupId, VersionInfo version, CodecType codecType) {
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest();
        getEncodingIdRequest.codecType(ModelHelper.encode(codecType))
                            .versionInfo(ModelHelper.encode(version));
        Response response = proxy.getOrGenerateEncodingId(groupId, getEncodingIdRequest);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("getEncodingId failed. Either Group or Version does not exist.");
        } else if (response.getStatus() == Response.Status.PRECONDITION_FAILED.getStatusCode()) {
            throw new CodecNotFoundException(String.format("Codec %s not registered.", codecType));
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public SchemaWithVersion getLatestSchema(String groupId, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getLatestSchemaForGroup(groupId);
        } else {
            return getLatestSchemaByObjectType(groupId, objectTypeName);
        }
    }

    @SneakyThrows
    private SchemaWithVersion getLatestSchemaForGroup(String groupId) {
        Response response = proxy.getLatestGroupSchema(groupId);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    @SneakyThrows
    private SchemaWithVersion getLatestSchemaByObjectType(String groupId, String objectTypeName) {
        Response response = proxy.getLatestSchemaForObjectType(groupId, objectTypeName);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("getLatestSchemaForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }

    private SchemaWithVersion processLatestSchemaResponse(Response response) {
        return ModelHelper.decode(response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class));
    }

    @Override
    public List<SchemaEvolution> getGroupEvolutionHistory(String groupId, @Nullable String objectTypeName) {
        if (objectTypeName == null) {
            return getEvolutionHistory(groupId);
        } else {
            return getEvolutionHistoryByObjectType(groupId, objectTypeName);
        }
    }

    @SneakyThrows
    private List<SchemaEvolution> getEvolutionHistory(String groupId) {
        Response response = proxy.getGroupSchemas(groupId);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("getEvolutionHistory failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    @SneakyThrows
    private List<SchemaEvolution> getEvolutionHistoryByObjectType(String groupId, String objectTypeName) {
        Response response = proxy.getObjectTypeSchemas(groupId, objectTypeName);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processHistoryResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("getEvolutionHistory failed. Either Group or Version does not exist.");
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

    @SneakyThrows
    @Override
    public VersionInfo getSchemaVersion(String groupId, SchemaInfo schema) {
        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schema));

        Response response = proxy.getSchemaVersion(groupId, getSchemaVersion);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema version.");
        }
    }

    @SneakyThrows
    @Override
    public boolean validateSchema(String groupId, SchemaInfo schema) {
        ValidateRequest validateRequest = new ValidateRequest()
                .schemaInfo(ModelHelper.encode(schema));
        Response response = proxy.validate(groupId, validateRequest);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Valid.class).isValid();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @SneakyThrows
    @Override
    public boolean canRead(String groupId, SchemaInfo schema) {
        CanReadRequest request = new CanReadRequest().schemaInfo(ModelHelper.encode(schema));
        Response response = proxy.canRead(groupId, request);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(CanRead.class).isCompatible();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @SneakyThrows
    @Override
    public List<CodecType> getCodecs(String groupId) {
        Response response = proxy.getCodecsList(groupId);
        CodecsList list = response.readEntity(CodecsList.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return list.getCodecTypes().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Failed to get codecs. Internal server error.");
        }
    }

    @SneakyThrows
    @Override
    public void addCodec(String groupId, CodecType codecType) {
        AddCodec addCodec = new AddCodec().codec(ModelHelper.encode(codecType));
        Response response = proxy.addCodec(groupId, addCodec);

        if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new NotFoundException("Group not found.");
        } else if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
            throw new RuntimeException("Failed to add codec. Internal server error.");
        }
    }

    @SneakyThrows
    private String encodeGroupId(String groupId) {
        return URLEncoder.encode(groupId, Charsets.UTF_8.toString());
    }
}
