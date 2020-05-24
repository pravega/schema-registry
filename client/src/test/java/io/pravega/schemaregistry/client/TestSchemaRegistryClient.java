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

import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.ResourceNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaNamesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestSchemaRegistryClient {
    @Test
    public void testGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        
        // add group
        // 1. success response code
        doReturn(response).when(proxy).createGroup(any());
        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        boolean addGroup = client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        assertTrue(addGroup);
        
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        addGroup = client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap());
        assertFalse(addGroup);

        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", 
                () -> client.addGroup("grp1", SchemaType.Avro, SchemaValidationRules.of(Compatibility.backward()), true, Collections.emptyMap()),
                e -> e instanceof RuntimeException);
        reset(response);
        
        // list groups
        doReturn(response).when(proxy).listGroups(null, 100);
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                                                       .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                                                               .schemaType(io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                                                       .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                                                       .versionedBySchemaName(false);
        String groupName = "mygroup";
        ListGroupsResponse groupList = new ListGroupsResponse().groups(Collections.singletonMap(groupName, mygroup));
        doReturn(groupList).when(response).readEntity(eq(ListGroupsResponse.class));

        Map<String, io.pravega.schemaregistry.contract.data.GroupProperties> groups = client.listGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.containsKey(groupName));
        assertEquals(groups.get(groupName).getSchemaType(), SchemaType.Any);
        assertEquals(groups.get(groupName).getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()), Compatibility.backward());

        reset(response);
    }

    @Test
    public void testListGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                                                       .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                                                               .schemaType(io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                                                       .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                                                       .versionedBySchemaName(false);
        String groupId = "mygroup";
        ListGroupsResponse groupList = new ListGroupsResponse().groups(Collections.singletonMap(groupId, mygroup));
        doReturn(response).when(proxy).listGroups(null, 100);

        doReturn(groupList).when(response).readEntity(eq(ListGroupsResponse.class));
        Map<String, io.pravega.schemaregistry.contract.data.GroupProperties> groups = client.listGroups();
        assertEquals(1, groups.size());
        assertTrue(groups.containsKey(groupId));
        assertEquals(groups.get(groupId).getSchemaType(), SchemaType.Any);
        assertEquals(groups.get(groupId).getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()), Compatibility.backward());
        
        // Runtime Exception
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", () -> client.listGroups(), e -> e instanceof RuntimeException);
    }

    @Test
    public void testRemoveGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).deleteGroup(anyString());
        doReturn(Response.Status.NO_CONTENT.getStatusCode()).when(response).getStatus();
        
        client.removeGroup("mygroup");
        
        // not OK response
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.removeGroup("mygroup"),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetGroupProperties() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getGroupProperties(anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup
                = new GroupProperties().properties(Collections.emptyMap())
                                                       .schemaType(new io.pravega.schemaregistry.contract.generated.rest.model.SchemaType()
                                                               .schemaType(
                                                                       io.pravega.schemaregistry.contract.generated.rest.model.SchemaType.SchemaTypeEnum.ANY))
                                                       .schemaValidationRules(ModelHelper.encode(SchemaValidationRules.of(Compatibility.backward())))
                                                       .versionedBySchemaName(false);
        doReturn(mygroup).when(response).readEntity(eq(GroupProperties.class));
        io.pravega.schemaregistry.contract.data.GroupProperties groupProperties = client.getGroupProperties("mygroup");
        assertEquals(groupProperties.getSchemaType(), SchemaType.Any);
        assertEquals(groupProperties.getSchemaValidationRules().getRules().get(Compatibility.class.getSimpleName()),
                Compatibility.backward());
        // ResourceNotFoundException
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testUpdateSchemaValidationRules() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).updateSchemaValidationRules(anyString(), any());

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
                e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateSchemaValidationRules("mygroup", schemaValidationRules),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testSchemaNamesApi() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaNames(anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        List<String> stringList = new ArrayList<>();
        stringList.add("element1");
        stringList.add("element2");
        SchemaNamesList schemaNamesList = new SchemaNamesList();
        schemaNamesList.objects(stringList);
        doReturn(schemaNamesList).when(response).readEntity(SchemaNamesList.class);
        List<String> output = client.getSchemaNames("mygroup");
        assertEquals(2, output.size());
        assertEquals("element1", output.get(0));
        assertEquals("element2", output.get(1));
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getSchemaNames("mygroup"),
                e -> e instanceof ResourceNotFoundException);
        // Runtime exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getSchemaNames("mygroup"),
                e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddSchema() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).addSchema(anyString(), any());
        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo versionInfo =
                new io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo().version(
                        5).schemaName("schema2").ordinal(5);
        doReturn(versionInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.addSchema("mygroup", schemaInfo);
        assertEquals(5, versionInfo1.getVersion());
        assertEquals("schema2", versionInfo1.getSchemaName());
        assertEquals(5, versionInfo1.getOrdinal());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
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
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaFromVersion(anyString(), anyInt());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaType schemaType = ModelHelper.encode(SchemaType.Any);
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = new io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo().schemaData(schemaData).schemaName("schema1").schemaType(schemaType).properties(properties);
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        doReturn(schemaInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class);
        SchemaInfo schemaInfo1 = client.getSchemaForVersion("mygroup", versionInfo);
        assertEquals(schemaInfo.getSchemaName(), schemaInfo1.getName());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaForVersion("mygroup", versionInfo), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaForVersion("mygroup", versionInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingInfo() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getEncodingInfo(anyString(), anyInt());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        CodecType codecType = CodecType.GZip;
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, codecType);
        EncodingId encodingId = new EncodingId(5);
        doReturn(ModelHelper.encode(encodingInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class);
        EncodingInfo encodingInfo1 = client.getEncodingInfo("mygroup", encodingId);
        assertEquals(encodingInfo.getCodec(), encodingInfo1.getCodec());
        assertEquals(encodingInfo.getSchemaInfo(), encodingInfo1.getSchemaInfo());
        assertEquals(encodingInfo.getVersionInfo(), encodingInfo1.getVersionInfo());
        // NotFound exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup", encodingId), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup", encodingId), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetEncodingId() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getEncodingId(anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        io.pravega.schemaregistry.contract.generated.rest.model.EncodingId encodingId = ModelHelper.encode(new EncodingId(5));
        doReturn(encodingId).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class);
        EncodingId encodingId1 = client.getEncodingId("mygroup", versionInfo, codecType);
        assertEquals(encodingId.getEncodingId().intValue(), encodingId1.getId());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof ResourceNotFoundException);
        // CodecTypeNotFound Exception
        doReturn(Response.Status.PRECONDITION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof CodecNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetLatestSchemaForGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getLatestSchema(anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doReturn(ModelHelper.encode(schemaWithVersion)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class);
        SchemaWithVersion schemaWithVersion1 = client.getLatestSchemaVersion("mygroup", null);
        assertEquals(schemaWithVersion.getSchema(), schemaWithVersion1.getSchema());
        assertEquals(schemaWithVersion.getVersion(), schemaWithVersion1.getVersion());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", null), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", null), e -> e instanceof RuntimeException);

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        versionInfo = new VersionInfo("schema2", 5, 5);
        schemaType = SchemaType.Any;
        schemaData = new byte[0];
        properties = new HashMap<>();
        schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doReturn(ModelHelper.encode(schemaWithVersion)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class);
        schemaWithVersion1 = client.getLatestSchemaVersion("mygroup", "myobject");
        assertEquals(schemaWithVersion.getSchema(), schemaWithVersion1.getSchema());
        assertEquals(schemaWithVersion.getVersion(), schemaWithVersion1.getVersion());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", "myobject"), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", "myobject"), e -> e instanceof RuntimeException);
    }
    
    @Test
    public void testGroupEvolutionHistory() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getGroupHistory(anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.backward());
        GroupHistoryRecord groupHistoryRecord = new io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord()
                .schemaInfo(ModelHelper.encode(schemaInfo)).version(ModelHelper.encode(versionInfo))
                .validationRules(ModelHelper.encode(schemaValidationRules)).timestamp(100L).schemaString("");
        GroupHistory history = new GroupHistory();
        history.addHistoryItem(groupHistoryRecord);
        doReturn(history).when(response).readEntity(GroupHistory.class);
        List<io.pravega.schemaregistry.contract.data.GroupHistoryRecord> groupHistoryList = client.getGroupHistory("mygroup");
        assertEquals(1, groupHistoryList.size());
        assertEquals(schemaValidationRules, groupHistoryList.get(0).getRules());
        assertEquals(schemaInfo, groupHistoryList.get(0).getSchema());
        assertEquals(versionInfo, groupHistoryList.get(0).getVersion());
        assertEquals(100L, groupHistoryList.get(0).getTimestamp());
        assertEquals("", groupHistoryList.get(0).getSchemaString());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupHistory("mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupHistory("mygroup"), e -> e instanceof RuntimeException);

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        versionInfo = new VersionInfo("schema2", 5, 5);
        schemaType = SchemaType.Any;
        schemaData = new byte[0];
        properties = new HashMap<>();
        schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion schemaVersionAndRules = new io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion()
                .schemaInfo(ModelHelper.encode(schemaInfo)).version(ModelHelper.encode(versionInfo));
        SchemaVersionsList schemaList = new SchemaVersionsList();
        schemaList.addSchemasItem(schemaVersionAndRules);
        doReturn(schemaList).when(response).readEntity(SchemaVersionsList.class);
    }

    @Test
    public void testGetSchemaVersion() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaVersion(anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        VersionInfo versionInfo = new VersionInfo("schema2", 5, 5);
        doReturn(ModelHelper.encode(versionInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.getVersionForSchema("mygroup", schemaInfo);
        assertEquals(versionInfo.getSchemaName(), versionInfo1.getSchemaName());
        assertEquals(versionInfo.getVersion(), versionInfo1.getVersion());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getVersionForSchema("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getVersionForSchema("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testValidateSchema() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).validate(anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        Valid valid = new Valid().valid(Boolean.TRUE);
        doReturn(valid).when(response).readEntity(Valid.class);
        Boolean valid1 = client.validateSchema("mygroup", schemaInfo);
        assertEquals(valid.isValid(), valid1);
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.validateSchema("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.validateSchema("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testCanRead() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).canRead(anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SchemaType schemaType = SchemaType.Any;
        byte[] schemaData = new byte[0];
        Map<String, String> properties = new HashMap<>();
        SchemaInfo schemaInfo = new SchemaInfo("schema1", schemaType, schemaData, properties);
        CanRead canRead = new CanRead().compatible(Boolean.TRUE);
        doReturn(canRead).when(response).readEntity(CanRead.class);
        Boolean canRead1 = client.canReadUsing("mygroup", schemaInfo);
        assertEquals(canRead.isCompatible(), canRead1);
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.canReadUsing("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.canReadUsing("mygroup", schemaInfo), e -> e instanceof RuntimeException);
    }

    @Test
    public void testGetCodecs() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getCodecsList(anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        CodecType codecType1 = CodecType.Snappy;
        CodecsList codecsList = new CodecsList();
        codecsList.addCodecTypesItem(ModelHelper.encode(codecType));
        codecsList.addCodecTypesItem(ModelHelper.encode(codecType1));
        doReturn(codecsList).when(response).readEntity(CodecsList.class);
        List<CodecType> codecsList1 = client.getCodecTypes("mygroup");
        assertEquals(2, codecsList1.size());
        assertEquals(CodecType.GZip, codecsList1.get(0));
        assertEquals(CodecType.Snappy, codecsList1.get(1));
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecTypes("mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecTypes("mygroup"), e -> e instanceof RuntimeException);
    }

    @Test
    public void testAddCodec() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).addCodec(anyString(), any());

        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        CodecType codecType = CodecType.GZip;
        client.addCodecType("mygroup", codecType);
        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodecType("mygroup", codecType), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodecType("mygroup", codecType), e -> e instanceof RuntimeException);
    }
}
