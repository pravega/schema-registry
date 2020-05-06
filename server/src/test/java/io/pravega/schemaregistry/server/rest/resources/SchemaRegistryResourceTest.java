/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
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
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.contract.exceptions.*;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.Version;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.pravega.schemaregistry.storage.StoreExceptions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SchemaRegistryResourceTest extends JerseyTest {
    public static final String GROUPS = "v1/groups";
    SchemaRegistryService service;

    @Override
    protected Application configure() {
        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        resourceObjs.add(new SchemaRegistryResourceImpl(service));

        final RegistryApplication application = new RegistryApplication(resourceObjs);
        return application;
    }

    @Test
    public void testListGroups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        GroupProperties group2 = new GroupProperties(SchemaType.Protobuf,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            map.put("group1", group1);
            map.put("group2", group2);
            return CompletableFuture.completedFuture(new MapWithToken<>(map, null));
        }).when(service).listGroups(eq(null));
        Future<Response> future = target(GROUPS).request().async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        GroupsList list = response.readEntity(GroupsList.class);
        assertEquals(list.getGroups().size(), 2);
        // Runtime Exception
        doAnswer(x ->
            Futures.failedFuture(new RuntimeException())
        ).when(service).listGroups(eq(null));
        response = target(GROUPS).request().async().get().get();
        assertEquals(response.getStatus(), 500);
    }

    @Test
    public void testCreateGroup() throws ExecutionException, InterruptedException {
        CreateGroupRequest createGroupRequest = new CreateGroupRequest().groupName("mygroup").properties(
                Collections.singletonMap("key", "value")).schemaType(
                ModelHelper.encode(SchemaType.Avro)).validateByObjectType(false).validationRules(
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
        assertEquals(200, response.getStatus());
        //GroupNotFound Exception
        // corresponding code not there in ResourceImpl
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).deleteGroup(anyString());
        response = target(GROUPS + "/" + groupName).request().async().delete().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetGroupProperties() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        GroupProperties group1 = new GroupProperties(SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()),
                false, Collections.singletonMap("Encode", Boolean.toString(false)));
        doAnswer(x -> CompletableFuture.completedFuture(group1)).when(service).getGroupProperties(anyString());
        Response response = target(GROUPS + "/" + groupName).request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SchemaType.Avro), response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties.class).getSchemaType());
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
                .schemaName("name")
                .schemaType(ModelHelper.encode(SchemaType.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap())
        );
        Future<Response> future = target(GROUPS + "/" + "mygroup" + "/schemas/canRead").request().async()
                .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).canRead(anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/canRead").request().async().post(
                Entity.entity(canReadRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).canRead(anyString(), any());
        response = target(GROUPS + "/" + "mygroup" + "/schemas/canRead").request().async()
                .post(Entity.entity(canReadRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testUpdateSchemaValidationRules() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(service).updateSchemaValidationRules(anyString(),
                any(), any());
        UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest =
                new UpdateValidationRulesPolicyRequest().validationRules(
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
    public void testGetGroupSchemas() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        SchemaEvolution schemaEvolution = new SchemaEvolution(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()));
        List<SchemaEvolution> schemaEvolutionList = new ArrayList<>();
        schemaEvolutionList.add(schemaEvolution);
        doAnswer(x -> CompletableFuture.completedFuture(schemaEvolutionList)).when(service).getGroupEvolutionHistory(
                anyString(), any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(5, response.readEntity(SchemaList.class).getSchemas().get(0).getVersion().getOrdinal().intValue());
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupEvolutionHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupEvolutionHistory(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas" + "/versions").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetLatestGroupSchema() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, new VersionInfo("myschema", 5, 5));
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(service).getLatestSchema(anyString(),
                any());
        String groupName = "mygroup";
        Response response = target(
                GROUPS + "/" + groupName + "/schemas" + "/versions" + "/latest").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class).getVersion().getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getLatestSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/versions/latest").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getLatestSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas" + "/versions" + "/latest").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testAddSchemaToGroupIfAbsent() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        AddSchemaToGroupRequest addSchemaToGroupRequest = new AddSchemaToGroupRequest().schemaInfo(
                ModelHelper.encode(schemaInfo));
        VersionInfo versionInfo = new VersionInfo("mystring", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).addSchema(anyString(), any());
        String groupName = "mygroup";
        Response response = target(GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(201, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).addSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
        // IncompatibleSchema Exception
        doAnswer(x ->
                Futures.failedFuture(new IncompatibleSchemaException("Incompatible Schema"))
        ).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(409, response.getStatus());
        // SchemaTypeMismatch Exception
        doAnswer(x ->
                Futures.failedFuture(new SchemaTypeMismatchException("Schema Type Mismatch Exception"))
        ).when(service).addSchema(anyString(), any());
        response = target(
                GROUPS + "/" + groupName + "/schemas").request().async().post(
                Entity.entity(addSchemaToGroupRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(417, response.getStatus());
    }

    @Test
    public void testValidate() throws ExecutionException, InterruptedException {
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        ValidateRequest validateRequest = new ValidateRequest().schemaInfo(
                ModelHelper.encode(schemaInfo)).validationRules(
                ModelHelper.encode(SchemaValidationRules.of(Compatibility.forward())));
        String groupName = "mygroup";
        doAnswer(x -> CompletableFuture.completedFuture(Boolean.TRUE)).when(service).validateSchema(anyString(), any());
        Response response = target(GROUPS + "/" + groupName + "/schemas" + "/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(Boolean.TRUE, response.readEntity(Valid.class).isValid());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).validateSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/validate").request().async().post(
                Entity.entity(validateRequest, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).validateSchema(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/validate").request().async().post(
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
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        doAnswer(x -> CompletableFuture.completedFuture(schemaInfo)).when(service).getSchema(anyString(), anyInt());
        Response response = target(
                GROUPS + "/" + groupName + "/schemas/versions/" + version).request().async().get().get();
        assertEquals(200, response.getStatus());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchema(anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/versions" + version).request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchema(anyString(), anyInt());
        response = target(GROUPS + "/" + groupName + "/schemas/versions" + version).request().async().get().get();
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
        int fingerprint = 5;
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        GetSchemaVersion getSchemaVersion = new GetSchemaVersion().schemaInfo(ModelHelper.encode(schemaInfo));
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        doAnswer(x -> CompletableFuture.completedFuture(versionInfo)).when(service).getSchemaVersion(anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/schemas/schema/" + fingerprint).request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class).getVersion().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getSchemaVersion(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + fingerprint).request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getSchemaVersion(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/schemas/schema/" + fingerprint).request().async().post(
                Entity.entity(getSchemaVersion, MediaType.APPLICATION_JSON)).get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetObjectTypeSchemas() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String objectTypeName = "myobject";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        SchemaEvolution schemaEvolution = new SchemaEvolution(schemaInfo, new VersionInfo("schemaName", 5, 5),
                SchemaValidationRules.of(Compatibility.allowAny()));
        List<SchemaEvolution> schemaEvolutionList = new ArrayList<>();
        schemaEvolutionList.add(schemaEvolution);
        doAnswer(x -> CompletableFuture.completedFuture(schemaEvolutionList)).when(service).getGroupEvolutionHistory(
                anyString(), anyString());
        Response response = target(
                GROUPS + "/" + groupName + "/objectTypes" + "/" + objectTypeName + "/schemas/versions").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(ModelHelper.encode(SchemaType.Avro),
                response.readEntity(SchemaList.class).getSchemas().get(0).getSchemaInfo().getSchemaType());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getGroupEvolutionHistory(anyString(), anyString());
        response = target(GROUPS + "/" + groupName + "/objectTypes" + "/" + objectTypeName + "/schemas/versions").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getGroupEvolutionHistory(anyString(), anyString());
        response = target(
                GROUPS + "/" + groupName + "/objectTypes" + "/" + objectTypeName + "/schemas/versions").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetObjectTypes() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String object = "groupObject";
        List<String> stringList = new ArrayList<>();
        stringList.add(object);
        ContinuationToken continuationToken = new ContinuationToken();
        ListWithToken<String> listWithToken = new ListWithToken<>(stringList, continuationToken);
        doAnswer(x -> CompletableFuture.completedFuture(listWithToken)).when(service).getObjectTypes(anyString(),
                any());
        Response response = target(GROUPS + "/" + groupName + "/objectTypes").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(object, response.readEntity(ObjectTypesList.class).getObjectTypes().get(0));
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getObjectTypes(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/objectTypes").request().async().get().get();
        assertEquals(404, response.getStatus());
        //Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getObjectTypes(anyString(), any());
        response = target(GROUPS + "/" + groupName + "/objectTypes").request().async().get().get();
        assertEquals(500, response.getStatus());
    }

    @Test
    public void testGetLatestSchemaForObjectType() throws ExecutionException, InterruptedException {
        String groupName = "mygroup";
        String object = "groupObject";
        byte[] schemaData = new byte[0];
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo =
                new io.pravega.schemaregistry.contract.data.SchemaInfo(
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
                        Collections.singletonMap("key", "value"));
        VersionInfo versionInfo = new VersionInfo("myschema", 5, 5);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doAnswer(x -> CompletableFuture.completedFuture(schemaWithVersion)).when(service).getLatestSchema(anyString(),
                anyString());
        Response response = target(
                GROUPS + "/" + groupName + "/objectTypes" + "/" + object + "/schemas/versions/latest").request().async().get().get();
        assertEquals(200, response.getStatus());
        assertEquals(5, response.readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class).getVersion().getOrdinal().intValue());
        // GroupNotFound Exception
        doAnswer(x -> Futures.failedFuture(StoreExceptions.create(Type.DATA_NOT_FOUND, "Group Not Found"))).when(
                service).getLatestSchema(anyString(), anyString());
        response = target(GROUPS + "/" + groupName + "/objectTypes" + "/" + object + "/schemas/versions/latest").request().async().get().get();
        assertEquals(404, response.getStatus());
        // Runtime Exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException())).when(service).getLatestSchema(anyString(), anyString());
        response = target(
                GROUPS + "/" + groupName + "/objectTypes" + "/" + object + "/schemas/versions/latest").request().async().get().get();
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
                        "schemaName", "anyObject", SchemaType.Avro, schemaData,
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
