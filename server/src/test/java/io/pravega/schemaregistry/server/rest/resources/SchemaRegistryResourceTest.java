/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.server.rest.filter.NamespaceRedirectFilter;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.StoreExceptions;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static io.pravega.schemaregistry.storage.StoreExceptions.Type;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    private static final String GROUPS = "v1/groups";
    private static final String NAMESPACE_FORMAT = "v1/namespace/%s/groups";
    private SchemaRegistryService service;
    private ScheduledExecutorService executor;

    @Override
    protected Application configure() {
        executor = Executors.newSingleThreadScheduledExecutor();
        forceSet(TestProperties.CONTAINER_PORT, "0");
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new NamespaceRedirectFilter());
        ServiceConfig config = ServiceConfig.builder().build();
        AuthHandlerManager authHandlerManager = new AuthHandlerManager(config);
        resourceObjs.add(new GroupResourceImpl(service, config, authHandlerManager, executor));
        resourceObjs.add(new SchemaResourceImpl(service, config, authHandlerManager, executor));

        return new RegistryApplication(resourceObjs);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testListGroups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(),
                false);
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            return CompletableFuture.completedFuture(new ResultPage<>(Lists.newArrayList(map.entrySet()), null));
        }).when(service).listGroups(any(), any(), anyInt());
        Future<Response> future = target(GROUPS).queryParam("limit", 100).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 1);
        
        // Runtime Exception
        doAnswer(x ->
                Futures.failedFuture(new RuntimeException())
        ).when(service).listGroups(any(), any(), anyInt());
        response = target(GROUPS).queryParam("limit", 100).request().async().get().get();
        assertEquals(response.getStatus(), 500);
    }

    @Test
    public void testCreateGroup() throws ExecutionException, InterruptedException {
        CreateGroupRequest createGroupRequest = new CreateGroupRequest().groupName("mygroup").groupProperties(
                ModelHelper.encode(
                        GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                                ImmutableMap.<String, String>builder().build()).serializationFormat(
                                SerializationFormat.Avro).compatibility(Compatibility.forward()).build()));
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(Boolean.TRUE));
        }).when(service).createGroup(any(), anyString(), any());
        Response response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        // Conflict
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(Boolean.FALSE));
        }).when(service).createGroup(any(), anyString(), any());
        response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // Runtime Exception
        doAnswer(x ->
                Futures.failedFuture(new RuntimeException())
        ).when(service).createGroup(any(), anyString(), any());
        response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testDeleteGroup() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).deleteGroup(any(), anyString());
        Response response = target(GROUPS + "/" + groupName).request().async().delete().get();
        assertEquals(204, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).deleteGroup(any(), anyString());
        response = target(GROUPS + "/" + groupName).request().async().delete().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupProperties() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GroupProperties group1 = GroupProperties.builder().allowMultipleTypes(Boolean.FALSE).properties(
                com.google.common.collect.ImmutableMap.<String, String>builder().build()).serializationFormat(
                SerializationFormat.Avro).compatibility(Compatibility.forward()).build();
        doAnswer(x -> CompletableFuture.completedFuture(group1)).when(service).getGroupProperties(any(), anyString());
        Response response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro), response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class).getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupProperties(any(), anyString());
        response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupProperties(any(),
                anyString());
        response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testCanRead() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(true)).when(service).canRead(any(), anyString(), any());
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo()
                .type("name")
                .serializationFormat(ModelHelper.encode(SerializationFormat.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap());
        Future<Response> future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                .post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).canRead(any(), anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/versions/canRead").request().async().post(
                Entity.entity(schemaInfo, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).canRead(any(), anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/versions/canRead").request().async()
                .post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testUpdateCompatibility() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).updateCompatibility(any(), anyString(),
                any(), any());
        UpdateCompatibilityRequest updateValidationRulesPolicyRequest =
                new UpdateCompatibilityRequest().compatibility(
                        ModelHelper.encode(Compatibility.backward())).previousCompatibility(null);
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/compatibility").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        // PreconditionFailed Exception Write Conflict
        doAnswer(x -> Futures.failedFuture(new PreconditionFailedException("Write Conflict"))).when(
                service).updateCompatibility(any(), anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/compatibility").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).updateCompatibility(any(), anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/compatibility").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).updateCompatibility(any(),
                anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/compatibility").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupSchemas() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                Compatibility.allowAny(), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                any(), anyString(), any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(5,
                response.readEntity(SchemaVersionsList.class).getSchemas().get(0).getVersionInfo().getId().intValue());
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testAddSchema() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("mystring", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).addSchema(any(), anyString(),
                any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).addSchema(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).addSchema(any(), anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
        // IncompatibleSchema Exception
        doAnswer(x ->
                Futures.failedFuture(new IncompatibleSchemaException("Incompatible Schema"))
        ).when(service).addSchema(any(), anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // SerializationFormatMismatch Exception
        doAnswer(x ->
                Futures.failedFuture(new SerializationFormatMismatchException("Schema Type Mismatch Exception"))
        ).when(service).addSchema(any(), anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(417, response.getStatus());
    }

    @Test
    public void testValidate() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        ValidateRequest validateRequest = new ValidateRequest().schemaInfo(
                ModelHelper.encode(schemaInfo)).compatibility(
                ModelHelper.encode(Compatibility.forward()));
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(Boolean.TRUE)).when(service).validateSchema(any(), anyString(),
                any(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions" + "/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(Boolean.TRUE, response.readEntity(Valid.class).isValid());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).validateSchema(any(), anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).validateSchema(any(), anyString(),
                any(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaFromVersion() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        int version = 7;
        String schemaName = "myschema";
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(service).getSchema(any(), anyString(),
                anyString(), anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().get().get();
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchema(any(), anyString(), anyString(), anyInt());
        response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchema(any(), anyString(),
                anyString(), anyInt());
        response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetEncodingId() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest().codecType(
                "gzip").versionInfo(ModelHelper.encode(new VersionInfo("myschema", 5, 5)));
        EncodingId encodingId = new EncodingId(10);
        doAnswer(x -> CompletableFuture.completedFuture(encodingId)).when(service).getEncodingId(any(), anyString(),
                any(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(10, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class).getEncodingId().intValue());
        // CodecNotFound Exception
        doAnswer(x -> Futures.failedFuture(new CodecTypeNotRegisteredException("CodecNotFound Exception"))).when(
                service).getEncodingId(any(), anyString(),
                any(), any());
        response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(412, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getEncodingId(any(), anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getEncodingId(any(), anyString(),
                any(), any());
        response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaVersion() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.custom("custom1"), ByteBuffer.wrap(schemaData),
                        com.google.common.collect.ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).getSchemaVersion(any(), anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemaVersion(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemaVersion(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupHistory() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                Compatibility.allowAny(), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                any(), anyString(), eq(null));
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro),
                response.readEntity(SchemaVersionsList.class).getSchemas().get(
                        0).getSchemaInfo().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(any(), anyString(), eq(null));
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(any(), anyString(),
                eq(null));
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemasForSchemaNames() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String schemaName = "schemaName";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                Compatibility.allowAny(), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                any(), anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName",
                schemaName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro),
                response.readEntity(SchemaVersionsList.class).getSchemas().get(
                        0).getSchemaInfo().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName",
                schemaName).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName",
                schemaName).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemas() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.custom("custom1"), ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        List<SchemaWithVersion> schemaWithVersionList = new ArrayList<>();
        schemaWithVersionList.add(schemaWithVersion);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersionList)).when(service).getSchemas(any(),
                anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas").request().async().get().get();
        List<io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion> schemaWithVersionList1 =
                response.readEntity(
                        SchemaVersionsList.class).getSchemas();
        assertEquals("schemaName", schemaWithVersionList1.get(0).getSchemaInfo().getType());
        assertEquals(1, schemaWithVersionList1.size());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemas(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemas(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetEncodingInfo() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        int encodingId = 7;
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, new CodecType("gzip"));
        doAnswer(x -> CompletableFuture.completedFuture(encodingInfo)).when(service).getEncodingInfo(any(), anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals("gzip", response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class).getCodecType().getName());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getEncodingInfo(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getEncodingInfo(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetCodecsList() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        List<CodecType> codecTypeList = new ArrayList<>();
        codecTypeList.add(new CodecType("gzip"));
        codecTypeList.add(new CodecType("snappy"));
        doAnswer(x -> CompletableFuture.completedFuture(codecTypeList)).when(service).getCodecTypes(any(), anyString());
        Response response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().get().get();
        assertEquals(200, response.getStatus());
        CodecTypes codecTypeList1 = response.readEntity(CodecTypes.class);
        assertEquals("snappy",
                codecTypeList1.getCodecTypes().get(1).getName());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getCodecTypes(any(), anyString());
        response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getCodecTypes(any(), anyString());
        response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testAddCodecType() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).addCodecType(any(), anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().post(
                Entity.entity(new CodecType("gzip"), MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).addCodecType(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().post(
                Entity.entity(new CodecType("gzip"), MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).addCodecType(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/codecTypes").request().async().post(
                Entity.entity(new CodecType("gzip"), MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaReferences() throws ExecutionException, InterruptedException {
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        schemaName, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo(schemaName, 5, 5);
        Map<String, VersionInfo> map = Collections.singletonMap("default", versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(map)).when(service).getSchemaReferences(any(), any());
        Response response = target("v1/schemas/addedTo").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertTrue(
                response.readEntity(AddedTo.class).getGroups().get("default").equals(ModelHelper.encode(versionInfo)));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemaReferences(any(), any());
        response = target("v1/schemas/addedTo").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemaReferences(any(), any());
        response = target("v1/schemas/addedTo").request().async().post(
                Entity.entity(ModelHelper.encode(schemaInfo), MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testDeleteSchemaVersion() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        schemaName, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo(schemaName, 5, 5);
        int version = versionInfo.getVersion();
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).deleteSchema(any(), anyString(),
                anyString(), anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().delete().get();
        assertEquals(204, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).deleteSchema(any(), anyString(), anyString(), anyInt());
        response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().delete().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).deleteSchema(any(), anyString(),
                anyString(), anyInt());
        response = target(
                GROUPS + "/" + groupName + "/schemas/" + schemaName + "/versions/" + version).request().async().delete().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testDeleteSchemaFromId() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        schemaName, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo(schemaName, 5, 5);
        int ordinal = versionInfo.getId();
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).deleteSchema(any(), anyString(), anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().delete().get();
        assertEquals(204, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).deleteSchema(any(), anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().delete().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).deleteSchema(any(), anyString(),
                anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().delete().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaFromId() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        schemaName, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo(schemaName, 5, 5);
        int ordinal = versionInfo.getId();
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(service).getSchema(any(), anyString(),
                anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().get().get();
        assertEquals(200, response.getStatus());
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo1 = response.readEntity(io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class);
        assertTrue(Objects.equals(schemaInfo1, ModelHelper.encode(schemaInfo)));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchema(any(), anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchema(any(), anyString(),
                anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + ordinal).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaVersions() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        List<GroupHistoryRecord> groupHistoryRecordList = new ArrayList<>();
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        schemaName, SerializationFormat.Avro, ByteBuffer.wrap(schemaData),
                        ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo(schemaName, 5, 5);
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, versionInfo,
                Compatibility.backward(), 100, "dummy");
        groupHistoryRecordList.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecordList)).when(service).getGroupHistory(any(),
                anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("type",
                schemaName).request().async().get().get();
        assertEquals(200, response.getStatus());
        List<io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion> schemaWithVersionList =
                response.readEntity(
                        SchemaVersionsList.class).getSchemas();
        assertEquals(1, schemaWithVersionList.size());
        assertTrue(schemaWithVersionList.get(0).getVersionInfo().equals(ModelHelper.encode(versionInfo)));
        assertTrue(schemaWithVersionList.get(0).getSchemaInfo().equals(ModelHelper.encode(schemaInfo)));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(any(), anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("type",
                schemaName).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(any(), anyString(),
                any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("type",
                schemaName).request().async().get().get();
        assertEquals(500, response.getStatus());
    }
}