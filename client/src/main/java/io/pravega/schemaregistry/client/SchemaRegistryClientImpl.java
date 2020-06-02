/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.ResourceNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private final ApiV1.GroupsApi groupProxy;

    SchemaRegistryClientImpl(URI uri) {
        Client client = ClientBuilder.newClient(new ClientConfig());
        this.groupProxy = WebResourceFactory.newResource(ApiV1.GroupsApi.class, client.target(uri));
    }

    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi groupProxy) {
        this.groupProxy = groupProxy;
    }

    @SneakyThrows
    @Override
    public boolean addGroup(String groupId, SerializationFormat serializationFormat, SchemaValidationRules validationRules, boolean allowMultipleTypes, Map<String, String> properties) {
        io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat serializationFormatModel = ModelHelper.encode(serializationFormat);

        io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules compatibility = ModelHelper.encode(validationRules);
        CreateGroupRequest request = new CreateGroupRequest().serializationFormat(serializationFormatModel)
                                                             .properties(properties).allowMultipleTypes(allowMultipleTypes)
                                                             .groupName(groupId)
                                                             .validationRules(compatibility);

        Response response = groupProxy.createGroup(request);
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
        Response response = groupProxy.deleteGroup(groupId);
        if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to remove the group.");
        }
    }

    @SneakyThrows
    @Override
    public Map<String, GroupProperties> listGroups() {
        String continuationToken = null;
        int limit = 100;
        Map<String, GroupProperties> result = new HashMap<>();
        while (true) {
            Response response = groupProxy.listGroups(continuationToken, limit);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new RuntimeException("Internal Service error. Failed to list groups.");
            }

            ListGroupsResponse entity = response.readEntity(ListGroupsResponse.class);
            Map<String, GroupProperties> map = entity.getGroups().entrySet().stream()
                                                     .collect(HashMap::new, (m, x) -> {
                        if (x.getValue() == null) {
                            m.put(x.getKey(), null);
                        } else {
                            SerializationFormat serializationFormat = ModelHelper.decode(x.getValue().getSerializationFormat());
                            SchemaValidationRules rules = ModelHelper.decode(x.getValue().getSchemaValidationRules());
                            m.put(x.getKey(), new GroupProperties(serializationFormat, rules, x.getValue().isAllowMultipleTypes(), x.getValue().getProperties()));
                        }
                    }, HashMap::putAll);
            continuationToken = entity.getContinuationToken();
            result.putAll(map);

            if (map.size() < 100) {
                break;
            }
        }
        return result;
    }

    @SneakyThrows
    @Override
    public GroupProperties getGroupProperties(String groupId) {
        Response response = groupProxy.getGroupProperties(groupId);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal error. Failed to get group properties.");
        }
    }

    @SneakyThrows
    @Override
    public void updateSchemaValidationRules(String groupId, SchemaValidationRules validationRules) {
        UpdateValidationRulesRequest request = new UpdateValidationRulesRequest()
                .validationRules(ModelHelper.encode(validationRules));

        Response response = groupProxy.updateSchemaValidationRules(groupId, request);
        if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new PreconditionFailedException("Conflict attempting to update the rules. Try again.");
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to update schema validation rules.");
        }
    }

    @SneakyThrows
    @Override
    public List<SchemaWithVersion> getSchemas(String groupId) {
        Response response = groupProxy.getSchemas(groupId);
        SchemaVersionsList objectsList = response.readEntity(SchemaVersionsList.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return objectsList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get object types.");
        }
    }

    @SneakyThrows
    @Override
    public VersionInfo addSchema(String groupId, SchemaInfo schema) {
        AddSchemaRequest addSchemaRequest = new AddSchemaRequest();
        addSchemaRequest.schemaInfo(ModelHelper.encode(schema));
        Response response = groupProxy.addSchema(groupId, addSchemaRequest);
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            throw new IncompatibleSchemaException("Schema is incompatible.");
        } else if (response.getStatus() == Response.Status.EXPECTATION_FAILED.getStatusCode()) {
            throw new SerializationFormatMismatchException("Schema type is invalid.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to addSchema.");
        }
    }

    @Override
    public void deleteSchemaVersion(String groupId, VersionInfo version) {
        Response response = groupProxy.deleteSchemaVersion(groupId, version.getOrdinal());
        if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Schema not found.");
        } else if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
            throw new RuntimeException("Internal Service error. Failed to get schema.");
        }
    }

    @SneakyThrows
    @Override
    public SchemaInfo getSchemaForVersion(String groupId, VersionInfo version) {
        Response response = groupProxy.getSchemaFromVersion(groupId, version.getOrdinal());
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema.");
        }
    }

    @SneakyThrows
    @Override
    public EncodingInfo getEncodingInfo(String groupId, EncodingId encodingId) {
        Response response = groupProxy.getEncodingInfo(groupId, encodingId.getId());
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Encoding not found.");
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
        Response response = groupProxy.getEncodingId(groupId, getEncodingIdRequest);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("getEncodingId failed. Either Group or Version does not exist.");
        } else if (response.getStatus() == Response.Status.PRECONDITION_FAILED.getStatusCode()) {
            throw new CodecNotFoundException(String.format("Codec %s not registered.", codecType));
        } else {
            throw new RuntimeException("Internal Service error. Failed to get encoding info.");
        }
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String groupId, @Nullable String type) {
        Response response = groupProxy.getLatestSchema(groupId, type);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return processLatestSchemaResponse(response);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("getLatestSchemaVersionForGroup failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get latest schema for group.");
        }
    }
    
    private SchemaWithVersion processLatestSchemaResponse(Response response) {
        return ModelHelper.decode(response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class));
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String groupId, @Nullable String type) {
        Response response = groupProxy.getSchemaVersions(groupId, type);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaVersionsList schemaList = response.readEntity(SchemaVersionsList.class);
            return schemaList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("getSchemas failed. Group does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema versions for group.");
        }
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String groupId) {
        Response response = groupProxy.getGroupHistory(groupId);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory history = response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory.class);
            return history.getHistory().stream().map(ModelHelper::decode).collect(Collectors.toList());

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("getSchemas failed. Either Group or Version does not exist.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema evolution history for group.");
        }
    }

    @SneakyThrows
    @Override
    public VersionInfo getVersionForSchema(String groupId, SchemaInfo schema) {
        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schema));

        Response response = groupProxy.getSchemaVersion(groupId, getSchemaVersion);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error. Failed to get schema version.");
        }
    }

    @SneakyThrows
    @Override
    public boolean validateSchema(String groupId, SchemaInfo schema) {
        ValidateRequest validateRequest = new ValidateRequest()
                .schemaInfo(ModelHelper.encode(schema));
        Response response = groupProxy.validate(groupId, validateRequest);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Valid.class).isValid();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @SneakyThrows
    @Override
    public boolean canReadUsing(String groupId, SchemaInfo schema) {
        CanReadRequest request = new CanReadRequest().schemaInfo(ModelHelper.encode(schema));
        Response response = groupProxy.canRead(groupId, request);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(CanRead.class).isCompatible();
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Schema not found.");
        } else {
            throw new RuntimeException("Internal Service error.");
        }
    }

    @SneakyThrows
    @Override
    public List<CodecType> getCodecTypes(String groupId) {
        Response response = groupProxy.getCodecsList(groupId);
        CodecsList list = response.readEntity(CodecsList.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return list.getCodecTypes().stream().map(ModelHelper::decode).collect(Collectors.toList());
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else {
            throw new RuntimeException("Failed to get codecs. Internal server error.");
        }
    }

    @SneakyThrows
    @Override
    public void addCodecType(String groupId, CodecType codecType) {
        AddCodec addCodec = new AddCodec().codec(ModelHelper.encode(codecType));
        Response response = groupProxy.addCodec(groupId, addCodec);

        if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new ResourceNotFoundException("Group not found.");
        } else if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
            throw new RuntimeException("Failed to add codec. Internal server error.");
        }
    }
}
