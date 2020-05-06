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

import io.pravega.schemaregistry.contract.data.*;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.*;
import io.pravega.schemaregistry.contract.generated.rest.model.*;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.IOP.Codec;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestSchemaRegistryClient {

    Client jerseyClient;
    WebTarget webTarget;
    Invocation.Builder invocation;
    SchemaRegistryClientImpl client;
    Response response;

    @Before
    public void setUp() {
        jerseyClient = mock(Client.class);
        webTarget = mock(WebTarget.class);
        invocation = mock(Invocation.Builder.class);

        doReturn(webTarget).when(jerseyClient).target(any(URI.class));
        doReturn(webTarget).when(webTarget).path(anyString());
        doReturn(invocation).when(webTarget).request(MediaType.APPLICATION_JSON);

        client = new SchemaRegistryClientImpl(URI.create("http://localhost:9092"), jerseyClient);
        response = mock(Response.class);
    }

    @Test
    public void testAddGroup() {
        // add group
        // 1. success response code
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        boolean addGroup = client.addGroup("grp1", SchemaType.Avro,
                SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        assertTrue(addGroup);
        // 2. Conflict
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        addGroup = client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true,
                Collections.emptyMap());
        assertFalse(addGroup);
        // 3. Internal Server Error
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown",
                () -> client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()),
                        true, Collections.emptyMap()),
                e -> e instanceof RuntimeException);
    }


    @Test
    public void testListGroup() {
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().groupName("mygroup").properties(Collections.emptyMap())
                .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                        .schemaType(io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                .validateByObjectType(false);
        GroupsList groupList = new GroupsList().groups(Collections.singletonList(mygroup));
        doReturn(groupList).when(response).readEntity(eq(GroupsList.class));
        Map<String, io.pravega.schemaregistry.contract.data.GroupProperties> groups = client.listGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.containsKey("mygroup"));
        assertEquals(groups.get("mygroup").getSchemaType(), SchemaType.Any);
        assertEquals(groups.get("mygroup").getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()), Compatibility.backward());
        // Runtime Exception
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", () -> client.listGroups(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testRemoveGroup() {
        doReturn(response).when(invocation).delete();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        client.removeGroup("mygroup");
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        // not OK response
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.removeGroup("mygroup"),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetGroupProperties() {
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().groupName("mygroup").properties(Collections.emptyMap())
                .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                        .schemaType(
                                io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                .validateByObjectType(false);
        doReturn(mygroup).when(response).readEntity(eq(GroupProperties.class));
        io.pravega.schemaregistry.contract.data.GroupProperties groupProperties = client.getGroupProperties("mygroup");
        assertEquals(groupProperties.getSchemaType(), SchemaType.Any);
        assertEquals(groupProperties.getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()),
                Compatibility.backward());
        // NotFoundException
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testUpdateSchemaValidationRules() {
        doReturn(response).when(invocation).put(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.backward());
        client.updateSchemaValidationRules("mygroup", schemaValidationRules);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        // Precondition Failed
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateSchemaValidationRules("mygroup", schemaValidationRules),
                e -> e instanceof PreconditionFailedException);
        // NotFound exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateSchemaValidationRules("mygroup", schemaValidationRules),
                e -> e instanceof NotFoundException);
        // Runtime Exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateSchemaValidationRules("mygroup", schemaValidationRules),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetObjectTypes() {
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        List<String> stringList = new ArrayList<>();
        stringList.add("element1");
        stringList.add("element2");
        ObjectTypesList objectTypesList = new ObjectTypesList();
        objectTypesList.objectTypes(stringList);
        doReturn(objectTypesList).when(response).readEntity(ObjectTypesList.class);
        List<String> output = client.getObjectTypes("mygroup");
        assertEquals(2, output.size());
        assertEquals("element1", output.get(0));
        assertEquals("element2", output.get(1));
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getObjectTypes("mygroup"),
                e -> e instanceof NotFoundException);
        // Runtime exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getObjectTypes("mygroup"),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddSchema() {
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", "anyObject", schemaType, schemaData, properties);
        io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo versionInfo =
                new io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo().version(
                        5).objectType("schema2").ordinal(5);
        doReturn(versionInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.addSchema("mygroup", schemaInfo);
        assertEquals(5, versionInfo1.getVersion());
        assertEquals("schema2", versionInfo1.getObjectType());
        assertEquals(5, versionInfo1.getOrdinal());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof NotFoundException);
        // SchemaIncompatible exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof IncompatibleSchemaException);
        // SchemaTypeInvalid Exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof SchemaTypeMismatchException);
        //Runtime Exception
        doReturn(Response.Status.BAD_GATEWAY.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchema() {
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaType schemaType = ModelHelper.encode(SchemaType.Any);
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo().schemaData(schemaData).schemaName("schema1").schemaType(schemaType).properties(properties);
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        doReturn(schemaInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class);
        SchemaInfo schemaInfo1 = client.getSchema("mygroup", versionInfo);
        assertEquals(schemaInfo.getSchemaName(),schemaInfo1.getName());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchema("mygroup", versionInfo), e -> e instanceof NotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchema("mygroup", versionInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingInfo(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        CodecType codecType = CodecType.GZip;
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo,schemaInfo,codecType);
        EncodingId encodingId = new EncodingId(5);
        doReturn(ModelHelper.encode(encodingInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class);
        EncodingInfo encodingInfo1 = client.getEncodingInfo("mygroup",encodingId);
        assertEquals(encodingInfo.getCodec(), encodingInfo1.getCodec());
        assertEquals(encodingInfo.getSchemaInfo(),encodingInfo1.getSchemaInfo());
        assertEquals(encodingInfo.getVersionInfo(), encodingInfo1.getVersionInfo());
        // NotFound exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup",encodingId), e -> e instanceof NotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup",encodingId), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingId(){
        doReturn(response).when(invocation).put(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        io.pravega.schemaregistry.contract.generated.rest.model.EncodingId encodingId = ModelHelper.encode(new EncodingId(5));
        doReturn(encodingId).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class);
        EncodingId encodingId1 = client.getEncodingId("mygroup",versionInfo,codecType);
        assertEquals(encodingId.getEncodingId().intValue(),encodingId1.getId());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup",versionInfo,codecType), e -> e instanceof NotFoundException);
        // CodecTypeNotFound Exception
        doReturn(Response.Status.PRECONDITION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup",versionInfo,codecType), e -> e instanceof CodecNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup",versionInfo,codecType), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetLatestSchemaForGroup(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doReturn(ModelHelper.encode(schemaWithVersion)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class);
        SchemaWithVersion schemaWithVersion1 = client.getLatestSchema("mygroup", null);
        assertEquals(schemaWithVersion.getSchema(), schemaWithVersion1.getSchema());
        assertEquals(schemaWithVersion.getVersion(), schemaWithVersion1.getVersion());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchema("mygroup", null), e -> e instanceof NotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchema("mygroup", null), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetLatestSchemaByObjectType(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doReturn(ModelHelper.encode(schemaWithVersion)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class);
        SchemaWithVersion schemaWithVersion1 = client.getLatestSchema("mygroup","myobject");
        assertEquals(schemaWithVersion.getSchema(), schemaWithVersion1.getSchema());
        assertEquals(schemaWithVersion.getVersion(), schemaWithVersion1.getVersion());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchema("mygroup", "myobject"), e -> e instanceof NotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchema("mygroup", "myobject"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEvolutionHistory(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.backward());
        SchemaVersionAndRules schemaVersionAndRules = new SchemaVersionAndRules().validationRules(ModelHelper.encode(schemaValidationRules)).schemaInfo(ModelHelper.encode(schemaInfo)).version(ModelHelper.encode(versionInfo));
        SchemaList schemaList = new SchemaList();
        schemaList.addSchemasItem(schemaVersionAndRules);
        doReturn(schemaList).when(response).readEntity(SchemaList.class);
        List<SchemaEvolution> schemaEvolutionList1 = client.getGroupEvolutionHistory("mygroup", null);
        assertEquals(1, schemaEvolutionList1.size());
        assertEquals(schemaValidationRules, schemaEvolutionList1.get(0).getRules());
        assertEquals(schemaInfo, schemaEvolutionList1.get(0).getSchema());
        assertEquals(versionInfo, schemaEvolutionList1.get(0).getVersion());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupEvolutionHistory("mygroup", null), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupEvolutionHistory("mygroup", null), e -> e instanceof RuntimeException);
    }

    @Test
    public void GetEvolutionHistoryByObjectType(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.backward());
        SchemaVersionAndRules schemaVersionAndRules = new SchemaVersionAndRules().validationRules(ModelHelper.encode(schemaValidationRules)).schemaInfo(ModelHelper.encode(schemaInfo)).version(ModelHelper.encode(versionInfo));
        SchemaList schemaList = new SchemaList();
        schemaList.addSchemasItem(schemaVersionAndRules);
        doReturn(schemaList).when(response).readEntity(SchemaList.class);
        List<SchemaEvolution> schemaEvolutionList1 = client.getGroupEvolutionHistory("mygroup", null);
        assertEquals(1, schemaEvolutionList1.size());
        assertEquals(schemaValidationRules, schemaEvolutionList1.get(0).getRules());
        assertEquals(schemaInfo, schemaEvolutionList1.get(0).getSchema());
        assertEquals(versionInfo, schemaEvolutionList1.get(0).getVersion());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupEvolutionHistory("mygroup", "myobject"), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupEvolutionHistory("mygroup", "myobject"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetSchemaVersion(){
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        VersionInfo versionInfo = new VersionInfo("schema2",5,5);
        doReturn(ModelHelper.encode(versionInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.getSchemaVersion("mygroup",schemaInfo);
        assertEquals(versionInfo.getObjectType(), versionInfo1.getObjectType());
        assertEquals(versionInfo.getVersion(), versionInfo1.getVersion());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaVersion("mygroup",schemaInfo), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaVersion("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testValidateSchema(){
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        Valid valid = new Valid().valid(Boolean.TRUE);
        doReturn(valid).when(response).readEntity(Valid.class);
        Boolean valid1 = client.validateSchema("mygroup", schemaInfo);
        assertEquals(valid.isValid(), valid1);
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.validateSchema("mygroup", schemaInfo), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.validateSchema("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testCanRead(){
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1","anyObject",schemaType,schemaData,properties);
        CanRead canRead = new CanRead().compatible(Boolean.TRUE);
        doReturn(canRead).when(response).readEntity(CanRead.class);
        Boolean canRead1 = client.canRead("mygroup", schemaInfo);
        assertEquals(canRead.isCompatible(), canRead1);
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.canRead("mygroup", schemaInfo), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.canRead("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetCodecs(){
        doReturn(response).when(invocation).get();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        CodecType codecType1 = CodecType.Snappy;
        CodecsList codecsList = new CodecsList();
        codecsList.addCodecTypesItem(ModelHelper.encode(codecType));
        codecsList.addCodecTypesItem(ModelHelper.encode(codecType1));
        doReturn(codecsList).when(response).readEntity(CodecsList.class);
        List<CodecType> codecsList1 = client.getCodecs("mygroup");
        assertEquals(2, codecsList1.size());
        assertEquals(CodecType.GZip, codecsList1.get(0));
        assertEquals(CodecType.Snappy, codecsList1.get(1));
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecs("mygroup"), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecs("mygroup"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddCodec(){
        doReturn(response).when(invocation).post(any());
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        client.addCodec("mygroup", codecType);
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodec("mygroup", codecType), e -> e instanceof NotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodec("mygroup", codecType), e -> e instanceof RuntimeException);
    }

    @After
    public void reSet() {
        reset(response);
        reset(invocation);
    }
}




















