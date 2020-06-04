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
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.proxy.WebResourceFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.client.exceptions.RegistryExceptions.*;

public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private static final int LIMIT = 100;
    private static final Retry.RetryAndThrowConditionally RETRY = Retry
            .withExpBackoff(100, 2, 10, 1000)
            .retryWhen(x -> Exceptions.unwrap(x) instanceof ConnectionException);
    private final ApiV1.GroupsApi groupProxy;
    private final ApiV1.SchemasApi schemaProxy;

    SchemaRegistryClientImpl(URI uri) {
        Client client = ClientBuilder.newClient(new ClientConfig());
        this.groupProxy = WebResourceFactory.newResource(ApiV1.GroupsApi.class, client.target(uri));
        this.schemaProxy = WebResourceFactory.newResource(ApiV1.SchemasApi.class, client.target(uri));
    }

    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi groupProxy) {
        this(groupProxy, null);
    }

    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi groupProxy, ApiV1.SchemasApi schemaProxy) {
        this.groupProxy = groupProxy;
        this.schemaProxy = schemaProxy;
    }

    @Override
    public boolean addGroup(String groupId, GroupProperties groupProperties) {
        return withRetry(() -> {
            CreateGroupRequest request = new CreateGroupRequest().groupName(groupId).groupProperties(ModelHelper.encode(groupProperties));
            Response response = groupProxy.createGroup(request);
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return true;
            } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                return false;
            } else if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                throw new BadArgumentException("Group properties invalid. Verify that schema validation rules include compatibility.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to add the group.");
            }
        });
    }

    @Override
    public void removeGroup(String groupId) {
        withRetry(() -> {
            Response response = groupProxy.deleteGroup(groupId);
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                throw new InternalServerError("Internal Service error. Failed to remove the group.");
            }
        });
    }

    @Override
    public Map<String, GroupProperties> listGroups() {
        String continuationToken = null;

        Map<String, GroupProperties> result = new HashMap<>();
        Map<String, GroupProperties> map;
        do {
            ListGroupsResponse entity = getListGroupsResponse(continuationToken);
            map = new HashMap<>();
            for (Map.Entry<String, io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties> entry : entity.getGroups().entrySet()) {
                if (entry.getValue() == null) {
                    map.put(entry.getKey(), null);
                } else {
                    ModelHelper.decode(entry.getValue().getSerializationFormat());
                    map.put(entry.getKey(), ModelHelper.decode(entry.getValue()));
                }
            }
            continuationToken = entity.getContinuationToken();
            result.putAll(map);

        } while (map.size() >= LIMIT);
        return result;
    }

    private ListGroupsResponse getListGroupsResponse(String continuationToken) {
        return withRetry(() -> {
            Response response = groupProxy.listGroups(continuationToken, LIMIT);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new InternalServerError("Internal Service error. Failed to list groups.");
            }

            return response.readEntity(ListGroupsResponse.class);
        });
    }

    @Override
    public GroupProperties getGroupProperties(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getGroupProperties(groupId);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else {
                throw new InternalServerError("Internal error. Failed to get group properties.");
            }
        });
    }

    @Override
    public void updateSchemaValidationRules(String groupId, SchemaValidationRules validationRules, @Nullable SchemaValidationRules previousRules) {
        withRetry(() -> {
            UpdateValidationRulesRequest request = new UpdateValidationRulesRequest()
                    .validationRules(ModelHelper.encode(validationRules));
            if (previousRules != null) {
                request.setPreviousRules(ModelHelper.encode(previousRules));
            }

            Response response = groupProxy.updateSchemaValidationRules(groupId, request);
            if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                throw new PreconditionFailedException("Conflict attempting to update the rules. Try again.");
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new InternalServerError("Internal Service error. Failed to update schema validation rules.");
            }
        });
    }

    @Override
    public List<SchemaWithVersion> getSchemas(String groupId) {
        return latestSchemas(groupId, null);
    }

    private List<SchemaWithVersion> latestSchemas(String groupId, String type) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemas(groupId, type);
            SchemaVersionsList objectsList = response.readEntity(SchemaVersionsList.class);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return objectsList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get object types.");
            }
        });
    }

    @Override
    public VersionInfo addSchema(String groupId, SchemaInfo schema) {
        return withRetry(() -> {
            Response response = groupProxy.addSchema(groupId, ModelHelper.encode(schema));
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
                throw new SchemaValidationFailedException("Schema is incompatible.");
            } else if (response.getStatus() == Response.Status.EXPECTATION_FAILED.getStatusCode()) {
                throw new SerializationMismatchException("Serialization format disallowed.");
            } else if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
                throw new MalformedSchemaException("Schema is malformed. Verify the schema data and type");
            } else {
                throw new InternalServerError("Internal Service error. Failed to addSchema.");
            }
        });
    }

    @Override
    public void deleteSchemaVersion(String groupId, VersionInfo version) {
        withRetry(() -> {
            Response response = groupProxy.deleteSchemaVersion(groupId, version.getOrdinal());
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                throw new InternalServerError("Internal Service error. Failed to get schema.");
            }
        });
    }

    @Override
    public SchemaInfo getSchemaForVersion(String groupId, VersionInfo version) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemaFromVersion(groupId, version.getOrdinal());
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Schema not found.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get schema.");
            }
        });
    }

    @Override
    public EncodingInfo getEncodingInfo(String groupId, EncodingId encodingId) {
        return withRetry(() -> {
            Response response = groupProxy.getEncodingInfo(groupId, encodingId.getId());
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Encoding not found.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get encoding info.");
            }
        });
    }

    @Override
    public EncodingId getEncodingId(String groupId, VersionInfo version, String codecType) {
        return withRetry(() -> {
            GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest();
            getEncodingIdRequest.codecType(codecType)
                                .versionInfo(ModelHelper.encode(version));
            Response response = groupProxy.getEncodingId(groupId, getEncodingIdRequest);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("getEncodingId failed. Either Group or Version does not exist.");
            } else if (response.getStatus() == Response.Status.PRECONDITION_FAILED.getStatusCode()) {
                throw new CodecTypeNotRegisteredException(String.format("Codec type %s not registered.", codecType));
            } else {
                throw new InternalServerError("Internal Service error. Failed to get encoding info.");
            }
        });
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String groupId, @Nullable String schemaType) {
        List<SchemaWithVersion> list = latestSchemas(groupId, schemaType);
        if (schemaType == null) {
            return list.stream().max(Comparator.comparingInt(x -> x.getVersion().getOrdinal())).orElse(null);
        } else {
            return list.get(0);
        }
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String groupId, @Nullable String schemaType) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemaVersions(groupId, schemaType);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                SchemaVersionsList schemaList = response.readEntity(SchemaVersionsList.class);
                return schemaList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("getSchemaVersions failed. Group does not exist.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get schema versions for group.");
            }
        });
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getGroupHistory(groupId);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory history = response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory.class);
                return history.getHistory().stream().map(ModelHelper::decode).collect(Collectors.toList());

            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("getGroupHistory failed. Either Group or Version does not exist.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get schema evolution history for group.");
            }
        });
    }

    @Override
    public Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws ResourceNotFoundException, UnauthorizedException {
        return withRetry(() -> {
            Response response = schemaProxy.getSchemaReferences(ModelHelper.encode(schemaInfo));
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                io.pravega.schemaregistry.contract.generated.rest.model.AddedTo addedTo = response
                        .readEntity(io.pravega.schemaregistry.contract.generated.rest.model.AddedTo.class);
                return addedTo.getGroups().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> ModelHelper.decode(x.getValue())));

            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("getSchemaReferences failed. Either Group or Version does not exist.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get schema evolution history for group.");
            }
        });
    }

    @Override
    public VersionInfo getVersionForSchema(String groupId, SchemaInfo schema) {
        return withRetry(() -> {
            io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = ModelHelper.encode(schema);

            Response response = groupProxy.getSchemaVersion(groupId, schemaInfo);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Schema not found.");
            } else {
                throw new InternalServerError("Internal Service error. Failed to get schema version.");
            }
        });
    }

    @Override
    public boolean validateSchema(String groupId, SchemaInfo schema) {
        return withRetry(() -> {
            ValidateRequest validateRequest = new ValidateRequest()
                    .schemaInfo(ModelHelper.encode(schema));
            Response response = groupProxy.validate(groupId, validateRequest);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(Valid.class).isValid();
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else {
                throw new InternalServerError("Internal Service error.");
            }
        });
    }

    @Override
    public boolean canReadUsing(String groupId, SchemaInfo schema) {
        return withRetry(() -> {
            io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo request = ModelHelper.encode(schema);
            Response response = groupProxy.canRead(groupId, request);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(CanRead.class).isCompatible();
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Schema not found.");
            } else {
                throw new InternalServerError("Internal Service error.");
            }
        });
    }

    @Override
    public List<String> getCodecTypes(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getCodecTypesList(groupId);
            CodecTypesList list = response.readEntity(CodecTypesList.class);

            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return list.getCodecTypes();
            } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else {
                throw new InternalServerError("Failed to get codecTypes. Internal server error.");
            }
        });
    }

    @Override
    public void addCodecType(String groupId, String codecType) {
        withRetry(() -> {
            Response response = groupProxy.addCodecType(groupId, codecType);

            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new InternalServerError("Failed to add codec type. Internal server error.");
            }
        });
    }

    private <T> T withRetry(Supplier<T> supplier) {
        return RETRY.run(supplier::get);
    }

    private void withRetry(Runnable runnable) {
        RETRY.run(() -> {
            runnable.run();
            return null;
        });
    }
}
