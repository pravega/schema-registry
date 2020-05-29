/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.pravega.schemaregistry.storage.StoreExceptions.Type;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryResourceTest extends JerseyTest {
    private static final String GROUPS = "v1/groups";
    private SchemaRegistryService service;

    @Override
    protected Application configure() {
        forceSet(TestProperties.CONTAINER_PORT, "0");
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new SchemaRegistryResourceImpl(service, ServiceConfig.builder().build()));

        return new RegistryApplication(resourceObjs);
    }

    @Test
    public void testListGroups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SerializationFormat.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        ContinuationToken continuationToken = ContinuationToken.EMPTY;
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", null);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, continuationToken));
        }).when(service).listGroups(any(), anyInt());

        Future<Response> future = target(GROUPS).queryParam("limit", 100).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 2);
        // Runtime Exception
        doAnswer(x ->
            Futures.failedFuture(new RuntimeException())
        ).when(service).listGroups(any(), anyInt());
        response = target(GROUPS).queryParam("limit", 100).request().async().get().get();
        assertEquals(response.getStatus(), 500);
    }

    @Test
    public void testCreateGroup() throws ExecutionException, InterruptedException {
        CreateGroupRequest createGroupRequest = new CreateGroupRequest().groupName("mygroup").properties(
                Collections.singletonMap("key", "value")).serializationFormat(
                ModelHelper.encode(SerializationFormat.Avro)).allowMultipleTypes(Boolean.FALSE).validationRules(
                ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())));
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(Boolean.TRUE));
        }).when(service).createGroup(anyString(), any());
        Response response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        // Conflict
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new Boolean(Boolean.FALSE));
        }).when(service).createGroup(anyString(), any());
        response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // Runtime Exception
        doAnswer(x ->
                Futures.failedFuture(new RuntimeException())
        ).when(service).createGroup(anyString(), any());
        response = target(GROUPS).request().async().post(
                Entity.entity(createGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testDeleteGroup() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).deleteGroup(anyString());
        Response response = target(GROUPS + "/" + groupName).request().async().delete().get();
        assertEquals(204, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).deleteGroup(anyString());
        response = target(GROUPS + "/" + groupName).request().async().delete().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupProperties() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GroupProperties group1 = new GroupProperties(SerializationFormat.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        doAnswer(x -> CompletableFuture.completedFuture(group1)).when(service).getGroupProperties(anyString());
        Response response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro), response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class).getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupProperties(anyString());
        response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupProperties(anyString());
        response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testCanRead() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(true)).when(service).canRead(anyString(), any());
        CanReadRequest canReadRequest = new CanReadRequest().schemaInfo(new SchemaInfo()
                .type("name")
                .serializationFormat(ModelHelper.encode(SerializationFormat.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap())
        );
        Future<Response> future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                                                       .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());
        canReadRequest = new CanReadRequest().schemaInfo(new SchemaInfo()
                .type("name")
                .schemaData(new byte[0])
                .properties(Collections.emptyMap())
        );
        future = target(GROUPS).path("mygroup").path("schemas/versions/canRead").request().async()
                                                .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON));
        response = future.get();
        assertEquals(400, response.getStatus());
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).canRead(anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/versions/canRead").request().async().post(
                Entity.entity(canReadRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).canRead(anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/versions/canRead").request().async()
                .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testUpdateSchemaValidationRules() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).updateSchemaValidationRules(anyString(),
                any(), any());
        UpdateValidationRulesRequest updateValidationRulesPolicyRequest =
                new UpdateValidationRulesRequest().validationRules(
                        ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward()))).previousRules(null);
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/rules").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        // PreconditionFailed Exception Write Conflict
        doAnswer(x -> Futures.failedFuture(new PreconditionFailedException("Write Conflict"))).when(
                service).updateSchemaValidationRules(anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/rules").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).updateSchemaValidationRules(anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/rules").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).updateSchemaValidationRules(anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/rules").request().async().put(
                Entity.entity(updateValidationRulesPolicyRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaValidationRules() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GroupProperties groupProperties = new GroupProperties(SerializationFormat.Custom, SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE, Collections.singletonMap("key", "value"));
        doAnswer(x -> CompletableFuture.completedFuture(groupProperties)).when(service).getGroupProperties(anyString());
        Response response = target(GROUPS + "/" + groupName + "/rules").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(SchemaValidationRules.of(Compatibility.backward()), ModelHelper.decode(response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules.class)));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupProperties(anyString());
        response = target(GROUPS + "/" + groupName + "/rules").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupProperties(anyString());
        response = target(GROUPS + "/" + groupName + "/rules").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupSchemas() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                anyString(), any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(5, response.readEntity(SchemaVersionsList.class).getSchemas().get(0).getVersion().getOrdinal().intValue());
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetLatestGroupSchema() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, new VersionInfo("myschema", 5, 5));
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(service).getGroupLatestSchemaVersion(anyString(),
                any());
        String groupName = "mygroup";
        Response response = target(
                GROUPS + "/" + groupName + "/schemas" + "/versions" + "/latest").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class).getVersion().getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupLatestSchemaVersion(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/latest").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupLatestSchemaVersion(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas" + "/versions" + "/latest").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testAddSchemaToGroupIfAbsent() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        AddSchemaRequest addSchemaToGroupRequest = new AddSchemaRequest().schemaInfo(
                ModelHelper.encode(schemaInfo));
        VersionInfo versionInfo = new VersionInfo("mystring", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).addSchema(anyString(), any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).addSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
        // IncompatibleSchema Exception
        doAnswer(x ->
                Futures.failedFuture(new IncompatibleSchemaException("Incompatible Schema"))
        ).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // SerializationFormatMismatch Exception
        doAnswer(x ->
                Futures.failedFuture(new SerializationFormatMismatchException("Schema Type Mismatch Exception"))
        ).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(417, response.getStatus());
    }

    @Test
    public void testValidate() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        ValidateRequest validateRequest = new ValidateRequest().schemaInfo(
                ModelHelper.encode(schemaInfo)).validationRules(
                ModelHelper.encode(SchemaValidationRules.of(Compatibility.forward())));
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(Boolean.TRUE)).when(service).validateSchema(anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions" + "/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(Boolean.TRUE, response.readEntity(Valid.class).isValid());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).validateSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).validateSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemaFromVersion() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        int version = 7;
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(service).getSchema(anyString(), anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/versions/" + version).request().async().get().get();
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchema(anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/" + version).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchema(anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/" + version).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetEncodingId() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GetEncodingIdRequest getEncodingIdRequest = new GetEncodingIdRequest().codecType(
                ModelHelper.encode(CodecType.GZip)).versionInfo(ModelHelper.encode(new VersionInfo("myschema", 5, 5)));
        EncodingId encodingId = new EncodingId(10);
        doAnswer(x -> CompletableFuture.completedFuture(encodingId)).when(service).getEncodingId(anyString(), any(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(10, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class).getEncodingId().intValue());
        // CodecNotFound Exception
        doAnswer(x -> Futures.failedFuture(new CodecNotFoundException("CodecNotFound Exception"))).when(service).getEncodingId(anyString(),
                any(), any());
        response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(412, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getEncodingId(anyString(), any(), any());
        response = target(GROUPS + "/" + groupName + "/encodings").request().async().put(
                Entity.entity(getEncodingIdRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getEncodingId(anyString(), any(), any());
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
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schemaInfo));
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).getSchemaVersion(anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemaVersion(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemaVersion(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/find").request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupHistory() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                anyString(), eq(null));
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro),
                response.readEntity(SchemaVersionsList.class).getSchemas().get(0).getSchemaInfo().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(anyString(), eq(null));
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(anyString(), eq(null));
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
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GroupHistoryRecord groupHistoryRecord = new GroupHistoryRecord(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()), 100, "describeSchema");
        List<GroupHistoryRecord> groupHistoryRecords = new ArrayList<>();
        groupHistoryRecords.add(groupHistoryRecord);
        doAnswer(x -> CompletableFuture.completedFuture(groupHistoryRecords)).when(service).getGroupHistory(
                anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName", schemaName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SerializationFormat.Avro),
                response.readEntity(SchemaVersionsList.class).getSchemas().get(0).getSchemaInfo().getSerializationFormat());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName", schemaName).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").queryParam("schemaName", schemaName).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetSchemas() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", SerializationFormat.Custom, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("objectType", 5, 7);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        List<SchemaWithVersion> schemaWithVersionList = new ArrayList<>();
        schemaWithVersionList.add(schemaWithVersion);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersionList)).when(service).getSchemas(anyString());
        Response response = target(GROUPS + "/" + groupName + "/schemas").request().async().get().get();
        assertEquals("schemaName", response.readEntity(SchemaVersionsList.class).getSchemas().get(0).getSchemaInfo().getType());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemas(anyString());
        response = target(GROUPS + "/" + groupName + "/schemas").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemas(anyString());
        response = target(GROUPS + "/" + groupName + "/schemas/names").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetLatestSchema() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "myschema", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(service).getGroupLatestSchemaVersion(anyString(),
                eq(null));
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class).getVersion().getOrdinal().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupLatestSchemaVersion(anyString(), eq(null));
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupLatestSchemaVersion(anyString(), eq(null));
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetLatestSchemaForSchemaName() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String schemaName = "myschema";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "myschema", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(service).getGroupLatestSchemaVersion(anyString(),
                any());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").queryParam("schemaName", schemaName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class).getVersion().getOrdinal().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupLatestSchemaVersion(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").queryParam("schemaName", schemaName).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupLatestSchemaVersion(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas/versions/latest").queryParam("schemaName", schemaName).request().async().get().get();
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
                        "schemaName", SerializationFormat.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, CodecType.GZip);
        doAnswer(x -> CompletableFuture.completedFuture(encodingInfo)).when(service).getEncodingInfo(anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(CodecType.GZip), response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class).getCodecType());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getEncodingInfo(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getEncodingInfo(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/encodings/" + encodingId).request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetCodecsList() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        List<CodecType> codecTypeList = new ArrayList<>();
        codecTypeList.add(CodecType.GZip);
        codecTypeList.add(CodecType.Snappy);
        doAnswer(x -> CompletableFuture.completedFuture(codecTypeList)).when(service).getCodecTypes(anyString());
        Response response = target(GROUPS + "/" + groupName + "/codecs").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(CodecType.Snappy),
                response.readEntity(CodecsList.class).getCodecTypes().get(1));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getCodecTypes(anyString());
        response = target(GROUPS + "/" + groupName + "/codecs").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getCodecTypes(anyString());
        response = target(GROUPS + "/" + groupName + "/codecs").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testAddCodec() throws ExecutionException, InterruptedException {
        AddCodec addCodec = new AddCodec().codec(ModelHelper.encode(CodecType.GZip));
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).addCodec(anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/codecs").request().async().post(
                Entity.entity(addCodec, MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).addCodec(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/codecs").request().async().post(
                Entity.entity(addCodec, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).addCodec(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/codecs").request().async().post(
                Entity.entity(addCodec, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }
}
