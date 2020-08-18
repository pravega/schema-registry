/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.common.util.CertificateUtils;
import io.pravega.schemaregistry.common.AuthHelper;
import io.pravega.schemaregistry.common.ContinuationTokenIterator;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypes;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateCompatibilityRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.proxy.WebResourceFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.client.exceptions.RegistryExceptions.*;


public class SchemaRegistryClientImpl implements SchemaRegistryClient {
    private static final Retry.RetryAndThrowConditionally RETRY = Retry
            .withExpBackoff(100, 2, 10, 1000)
            .retryWhen(x -> Exceptions.unwrap(x) instanceof ConnectionException);
    private static final int GROUP_LIMIT = 100;
    public static final String HTTPS = "https";

    private final ApiV1.GroupsApi groupProxy;
    private final ApiV1.SchemasApi schemaProxy;
    private final String namespace;
    private final Client client;
    
    SchemaRegistryClientImpl(SchemaRegistryClientConfig config, String namespace) {
        ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(new ClientConfig());
        if (HTTPS.equalsIgnoreCase(config.getSchemaRegistryUri().getScheme())) {
            clientBuilder = clientBuilder.sslContext(getSSLContext(config));
            if (!config.isValidateHostName()) {
                clientBuilder.hostnameVerifier((a, b) -> true);
            }
        } 
        
        client = clientBuilder.build();
        
        if (config.isAuthEnabled()) {
            client.register((ClientRequestFilter) context -> {
                context.getHeaders().add(HttpHeaders.AUTHORIZATION, 
                        AuthHelper.getAuthorizationHeader(config.getAuthMethod(), config.getAuthToken()));
            });
        }
        this.namespace = namespace;
        this.groupProxy = WebResourceFactory.newResource(ApiV1.GroupsApi.class, client.target(config.getSchemaRegistryUri()));
        this.schemaProxy = WebResourceFactory.newResource(ApiV1.SchemasApi.class, client.target(config.getSchemaRegistryUri()));
    }
    
    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi groupProxy) {
        this(groupProxy, null);
    }

    @VisibleForTesting
    SchemaRegistryClientImpl(ApiV1.GroupsApi groupProxy, ApiV1.SchemasApi schemaProxy) {
        this.groupProxy = groupProxy;
        this.schemaProxy = schemaProxy;
        this.namespace = null;
        this.client = null;
    }

    @Override
    public boolean addGroup(String groupId, GroupProperties groupProperties) {
        return withRetry(() -> {
            CreateGroupRequest request = new CreateGroupRequest().groupName(groupId).groupProperties(ModelHelper.encode(groupProperties));
            Response response = groupProxy.createGroup(namespace, request);
            Response.Status status = Response.Status.fromStatusCode(response.getStatus());
            switch (status) {
                case CREATED:
                    return true;
                case CONFLICT:
                    return false;
                case BAD_REQUEST:
                    throw new BadArgumentException("Group properties invalid.");
                default:
                    return handleResponse(status, "Internal Service error. Failed to add the group.");
            }
        });
    }

    @Override
    public void removeGroup(String groupId) {
        withRetry(() -> {
            Response response = groupProxy.deleteGroup(namespace, groupId);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case NO_CONTENT:
                    return;
                default:
                    handleResponse(Response.Status.fromStatusCode(response.getStatus()), "Internal Service error. Failed to remove the group.");
            }
        });
    }

    @Override
    public Iterator<Map.Entry<String, GroupProperties>> listGroups() {
        final Function<String, Map.Entry<String, Collection<Map.Entry<String, GroupProperties>>>> function =
                continuationToken -> {
                    ListGroupsResponse entity = getListGroupsResponse(continuationToken);
                    List<Map.Entry<String, GroupProperties>> map = new LinkedList<>();
                    for (Map.Entry<String, io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties> entry : entity.getGroups().entrySet()) {
                        ModelHelper.decode(entry.getValue().getSerializationFormat());
                        map.add(new AbstractMap.SimpleEntry<>(entry.getKey(), ModelHelper.decode(entry.getValue())));
                    }
                    return new AbstractMap.SimpleEntry<>(entity.getContinuationToken(), map);
                };

        return new ContinuationTokenIterator<>(function, null);
    }

    private ListGroupsResponse getListGroupsResponse(String continuationToken) {
        return withRetry(() -> {
            Response response = groupProxy.listGroups(namespace, continuationToken, GROUP_LIMIT);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return response.readEntity(ListGroupsResponse.class);
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()), "Internal Service error. Failed to list groups.");
            }
        });
    }

    @Override
    public GroupProperties getGroupProperties(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getGroupProperties(namespace, groupId);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()), "Internal Service error. Failed to list groups.");
            }
        });
    }

    @Override
    public boolean updateCompatibility(String groupId, Compatibility compatibility, @Nullable Compatibility previous) {
        return withRetry(() -> {
            UpdateCompatibilityRequest request = new UpdateCompatibilityRequest()
                    .compatibility(ModelHelper.encode(compatibility));
            if (previous != null) {
                request.setPreviousCompatibility(ModelHelper.encode(previous));
            }

            Response response = groupProxy.updateCompatibility(namespace, groupId, request);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case CONFLICT:
                    return false;
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                case OK:
                    return true;
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to update compatibility.");
            }
        });
    }

    @Override
    public List<SchemaWithVersion> getSchemas(String groupId) {
        return latestSchemas(groupId, null);
    }

    private List<SchemaWithVersion> latestSchemas(String groupId, String type) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemas(namespace, groupId, type);
            SchemaVersionsList objectsList = response.readEntity(SchemaVersionsList.class);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return objectsList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get object types.");
            }
        });
    }

    @Override
    public VersionInfo addSchema(String groupId, SchemaInfo schemaInfo) {
        return withRetry(() -> {
            Response response = groupProxy.addSchema(namespace, groupId, ModelHelper.encode(schemaInfo));
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case CREATED:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                case CONFLICT:
                    throw new SchemaValidationFailedException("Schema is incompatible.");
                case EXPECTATION_FAILED:
                    throw new SerializationMismatchException("Serialization format disallowed.");
                case BAD_REQUEST:
                    throw new MalformedSchemaException("Schema is malformed. Verify the schema data and type");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to addSchema.");
            }
        });
    }

    @Override
    public void deleteSchemaVersion(String groupId, VersionInfo versionInfo) {
        withRetry(() -> {
            Response response = groupProxy.deleteSchemaForId(namespace, groupId, versionInfo.getId());
            if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new ResourceNotFoundException("Group not found.");
            } else if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                        "Internal Service error. Failed to get schema.");
            }
        });
    }

    @Override
    public SchemaInfo getSchemaForVersion(String groupId, VersionInfo versionInfo) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemaForId(namespace, groupId, versionInfo.getId());
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Schema not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get schema.");
            }
        });
    }

    @Override
    public EncodingInfo getEncodingInfo(String groupId, EncodingId encodingId) {
        return withRetry(() -> {
            Response response = groupProxy.getEncodingInfo(namespace, groupId, encodingId.getId());
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Encoding not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get encoding info.");
            }
        });
    }

    @Override
    public EncodingId getEncodingId(String groupId, VersionInfo versionInfo, String codecType) {
        return withRetry(() -> {
            GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest();
            getEncodingIdRequest.codecType(codecType)
                                .versionInfo(ModelHelper.encode(versionInfo));
            Response response = groupProxy.getEncodingId(namespace, groupId, getEncodingIdRequest);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("getEncodingId failed. Either Group or Version does not exist.");
                case PRECONDITION_FAILED:
                    throw new CodecTypeNotRegisteredException(String.format("Codec type %s not registered.", codecType));
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get encoding info.");
            }
        });
    }

    @Override
    public SchemaWithVersion getLatestSchemaVersion(String groupId, @Nullable String schemaType) {
        List<SchemaWithVersion> list = latestSchemas(groupId, schemaType);
        if (schemaType == null) {
            return list.stream().max(Comparator.comparingInt(x -> x.getVersionInfo().getId())).orElse(null);
        } else {
            return list.get(0);
        }
    }

    @Override
    public List<SchemaWithVersion> getSchemaVersions(String groupId, @Nullable String schemaType) {
        return withRetry(() -> {
            Response response = groupProxy.getSchemaVersions(namespace, groupId, schemaType);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    SchemaVersionsList schemaList = response.readEntity(SchemaVersionsList.class);
                    return schemaList.getSchemas().stream().map(ModelHelper::decode).collect(Collectors.toList());
                case NOT_FOUND:
                    throw new ResourceNotFoundException("getSchemaVersions failed. Group does not exist.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get schema versions for group.");
            }
        });
    }

    @Override
    public List<GroupHistoryRecord> getGroupHistory(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getGroupHistory(namespace, groupId);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory history = response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory.class);
                    return history.getHistory().stream().map(ModelHelper::decode).collect(Collectors.toList());
                case NOT_FOUND:
                    throw new ResourceNotFoundException("getGroupHistory failed. Either Group or Version does not exist.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get schema evolution history for group.");
            }
        });
    }

    @Override
    public Map<String, VersionInfo> getSchemaReferences(SchemaInfo schemaInfo) throws ResourceNotFoundException, UnauthorizedException {
        return withRetry(() -> {
            Response response = schemaProxy.getSchemaReferences(ModelHelper.encode(schemaInfo), namespace);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    io.pravega.schemaregistry.contract.generated.rest.model.AddedTo addedTo = response
                            .readEntity(io.pravega.schemaregistry.contract.generated.rest.model.AddedTo.class);
                    return addedTo.getGroups().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> ModelHelper.decode(x.getValue())));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("getSchemaReferences failed. Either Group or Version does not exist.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get schema evolution history for group.");
            }
        });
    }

    @Override
    public VersionInfo getVersionForSchema(String groupId, SchemaInfo schema) {
        return withRetry(() -> {
            io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = ModelHelper.encode(schema);

            Response response = groupProxy.getSchemaVersion(namespace, groupId, schemaInfo);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return ModelHelper.decode(response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class));
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Schema not registered.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error. Failed to get schema version.");
            }
        });
    }

    @Override
    public boolean validateSchema(String groupId, SchemaInfo schemaInfo) {
        return withRetry(() -> {
            ValidateRequest validateRequest = new ValidateRequest()
                    .schemaInfo(ModelHelper.encode(schemaInfo));
            Response response = groupProxy.validate(namespace, groupId, validateRequest);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return response.readEntity(Valid.class).isValid();
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error.");
            }
        });
    }

    @Override
    public boolean canReadUsing(String groupId, SchemaInfo schemaInfo) {
        return withRetry(() -> {
            io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo request = ModelHelper.encode(schemaInfo);
            Response response = groupProxy.canRead(namespace, groupId, request);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return response.readEntity(CanRead.class).isCompatible();
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Schema not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Internal Service error.");
            }
        });
    }

    @Override
    public List<CodecType> getCodecTypes(String groupId) {
        return withRetry(() -> {
            Response response = groupProxy.getCodecTypesList(namespace, groupId);
            CodecTypes list = response.readEntity(CodecTypes.class);
            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case OK:
                    return list.getCodecTypes().stream().map(ModelHelper::decode).collect(Collectors.toList());
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                default:
                    return handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Failed to get codecTypes. Internal server error.");
            }
        });
    }

    @Override
    public void addCodecType(String groupId, CodecType codecType) {
        withRetry(() -> {
            Response response = groupProxy.addCodecType(namespace, groupId, ModelHelper.encode(codecType));

            switch (Response.Status.fromStatusCode(response.getStatus())) {
                case CREATED:
                    return;
                case NOT_FOUND:
                    throw new ResourceNotFoundException("Group not found.");
                default:
                    handleResponse(Response.Status.fromStatusCode(response.getStatus()),
                            "Failed to add codec type. Internal server error.");
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

    private <T> T handleResponse(Response.Status status, String errorMessage) {
        switch (status) {
            case UNAUTHORIZED:
            case FORBIDDEN:
                throw new UnauthorizedException("User not authorized.");
            default:
                throw new InternalServerError(errorMessage);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    private SSLContext getSSLContext(SchemaRegistryClientConfig config) {
        try {
            KeyStore trustStore;
            if (config.isSystemPropTls()) {
                return SSLContext.getDefault();
            } else if (config.isCertificateTrustStore()) {
                trustStore = CertificateUtils.createTrustStore(config.getTrustStore());
            } else {
                trustStore = KeyStore.getInstance(config.getTrustStoreType());
                FileInputStream fin = new FileInputStream(config.getTrustStore());
                String trustStorePassword = config.getTrustStorePassword();
                if (trustStorePassword != null) {
                    trustStore.load(fin, trustStorePassword.toCharArray());
                } else {
                    trustStore.load(fin, null);
                }
            }
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(trustStore);
            SSLContext tlsContext = SSLContext.getInstance("TLS");
            tlsContext.init(null, factory.getTrustManagers(), null);
            return tlsContext;
        } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException |
                CertificateException | IOException e) {
            throw new IllegalArgumentException("Failure initializing trust store", e);
        }
    }
}
