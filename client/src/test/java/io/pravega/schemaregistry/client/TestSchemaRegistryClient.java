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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypes;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupProperties;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.pravega.schemaregistry.client.exceptions.RegistryExceptions.*;
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
        io.pravega.schemaregistry.contract.data.GroupProperties groupProperties = new io.pravega.schemaregistry.contract.data.GroupProperties(
                SerializationFormat.Avro, Compatibility.backward(), true);
        doReturn(response).when(proxy).createGroup(any(), any());
        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        boolean addGroup = client.addGroup("grp1", groupProperties);
        assertTrue(addGroup);
        
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        addGroup = client.addGroup("grp1", groupProperties);
        assertFalse(addGroup);

        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", 
                () -> client.addGroup("grp1", groupProperties),
                e -> e instanceof InternalServerError);
        reset(response);
        
        // list groups
        doReturn(response).when(proxy).listGroups(null, null, 100);
        Response response2 = mock(Response.class);
        doReturn(response2).when(proxy).listGroups(null, "token", 100);
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        doReturn(Response.Status.OK.getStatusCode()).when(response2).getStatus();
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                                                       .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                                                               .serializationFormat(io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat.SerializationFormatEnum.ANY))
                                                       .compatibility(ModelHelper.encode(Compatibility.backward()))
                                                       .allowMultipleTypes(false);
        String groupName = "mygroup";
        ListGroupsResponse groupList = new ListGroupsResponse().groups(Collections.singletonMap(groupName, mygroup)).continuationToken("token");
        doReturn(groupList).when(response).readEntity(eq(ListGroupsResponse.class));
        doReturn(new ListGroupsResponse().groups(Collections.emptyMap()).continuationToken("token")).when(response2).readEntity(eq(ListGroupsResponse.class));

        val groups = Lists.newArrayList(client.listGroups());
        assertEquals(1, groups.size());
        assertTrue(groups.stream().anyMatch(x -> x.getKey().equals(groupName)));
        Map.Entry<String, io.pravega.schemaregistry.contract.data.GroupProperties> group = 
                groups.stream().filter(x -> x.getKey().equals(groupName)).findAny().orElseThrow(RuntimeException::new);
        assertEquals(group.getValue().getSerializationFormat(), SerializationFormat.Any);
        assertEquals(group.getValue().getCompatibility(), Compatibility.backward());

        reset(response);
    }

    @Test
    public void testListGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup = new GroupProperties().properties(Collections.emptyMap())
                                                       .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                                                               .serializationFormat(io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat.SerializationFormatEnum.ANY))
                                                       .compatibility(ModelHelper.encode(Compatibility.backward()))
                                                       .allowMultipleTypes(false);
        String groupId = "mygroup";
        ListGroupsResponse groupList = new ListGroupsResponse().groups(Collections.singletonMap(groupId, mygroup)).continuationToken("token");
        ListGroupsResponse groupList2 = new ListGroupsResponse().groups(Collections.emptyMap()).continuationToken("token");
        doReturn(response).when(proxy).listGroups(null, null, 100);
        Response response2 = mock(Response.class);
        doReturn(response2).when(proxy).listGroups(null, "token", 100);
        doReturn(Response.Status.OK.getStatusCode()).when(response2).getStatus();

        doReturn(groupList).when(response).readEntity(eq(ListGroupsResponse.class));
        doReturn(groupList2).when(response2).readEntity(eq(ListGroupsResponse.class));
        val groups = Lists.newArrayList(client.listGroups());
        assertEquals(1, groups.size());
        assertTrue(groups.stream().anyMatch(x -> x.getKey().equals(groupId)));
        Map.Entry<String, io.pravega.schemaregistry.contract.data.GroupProperties> group =
                groups.stream().filter(x -> x.getKey().equals(groupId)).findAny().orElseThrow(RuntimeException::new);
        assertEquals(group.getValue().getSerializationFormat(), SerializationFormat.Any);
        assertEquals(group.getValue().getCompatibility(), Compatibility.backward());
        
        // Runtime Exception
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("Exception should have been thrown", () -> Lists.newArrayList(client.listGroups()), e -> e instanceof InternalServerError);
    }

    @Test
    public void testRemoveGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).deleteGroup(any(), anyString());
        doReturn(Response.Status.NO_CONTENT.getStatusCode()).when(response).getStatus();
        
        client.removeGroup("mygroup");
        
        // not OK response
        doReturn(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.removeGroup("mygroup"),
                e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetGroupProperties() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getGroupProperties(any(), anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        GroupProperties mygroup
                = new GroupProperties().properties(Collections.emptyMap())
                                                       .serializationFormat(new io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat()
                                                               .serializationFormat(
                                                                       io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat.SerializationFormatEnum.ANY))
                                                       .compatibility(ModelHelper.encode(Compatibility.backward()))
                                                       .allowMultipleTypes(false);
        doReturn(mygroup).when(response).readEntity(eq(GroupProperties.class));
        io.pravega.schemaregistry.contract.data.GroupProperties groupProperties = client.getGroupProperties("mygroup");
        assertEquals(groupProperties.getSerializationFormat(), SerializationFormat.Any);
        assertEquals(groupProperties.getCompatibility(), Compatibility.backward());
        // ResourceNotFoundException
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getGroupProperties(
                "mygroup"), e -> e instanceof InternalServerError);
    }

    @Test
    public void testUpdateCompatibility() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).updateCompatibility(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        Compatibility compatibility = Compatibility.backward();
        client.updateCompatibility("mygroup", compatibility, null);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        // Precondition Failed
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        assertFalse(client.updateCompatibility("mygroup", compatibility, null));
        // NotFound exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateCompatibility("mygroup", compatibility, null),
                e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.updateCompatibility("mygroup", compatibility, null),
                e -> e instanceof InternalServerError);
    }

    @Test
    public void testSchemasApi() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemas(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("schema1", serializationFormat.getFullTypeName(), 5, 5);
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion schemaVersion = new io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion()
                .schemaInfo(ModelHelper.encode(schemaInfo)).versionInfo(ModelHelper.encode(versionInfo));
        SchemaVersionsList schemaList = new SchemaVersionsList();
        schemaList.addSchemasItem(schemaVersion);
        doReturn(schemaList).when(response).readEntity(SchemaVersionsList.class);
        List<SchemaWithVersion> output = client.getSchemas("mygroup");
        assertEquals(1, output.size());
        assertEquals("schema1", output.get(0).getSchemaInfo().getType());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getSchemas("mygroup"),
                e -> e instanceof ResourceNotFoundException);
        // Runtime exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown", () -> client.getSchemas("mygroup"),
                e -> e instanceof InternalServerError);
    }

    @Test
    public void testAddSchema() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).addSchema(any(), anyString(), any());
        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo versionInfo =
                new io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo()
                        .serializationFormat("a").version(5).type("schema2").id(5);
        doReturn(versionInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.addSchema("mygroup", schemaInfo);
        assertEquals(5, versionInfo1.getVersion());
        assertEquals("schema2", versionInfo1.getType());
        assertEquals(5, versionInfo1.getId());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
        // SchemaIncompatible exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof SchemaValidationFailedException);
        // SerializationFormatInvalid Exception
        doReturn(Response.Status.EXPECTATION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof SerializationMismatchException);
        //Runtime Exception
        doReturn(Response.Status.BAD_GATEWAY.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addSchema("mygroup", schemaInfo), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetSchema() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaForId(any(), anyString(), anyInt());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        io.pravega.schemaregistry.contract.generated.rest.model.SerializationFormat serializationFormat = ModelHelper.encode(SerializationFormat.custom("custom"));
        byte[] schemaData = new byte[0];
        
        io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo schemaInfo = 
                new io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo()
                        .schemaData(schemaData).type("schema1").serializationFormat(serializationFormat).properties(Collections.emptyMap());
        VersionInfo versionInfo = new VersionInfo("schema2", serializationFormat.getFullTypeName(), 5, 5);
        doReturn(schemaInfo).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo.class);
        SchemaInfo schemaInfo1 = client.getSchemaForVersion("mygroup", versionInfo);
        assertEquals(schemaInfo.getType(), schemaInfo1.getType());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaForVersion("mygroup", versionInfo), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getSchemaForVersion("mygroup", versionInfo), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetEncodingInfo() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getEncodingInfo(any(), anyString(), anyInt());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        VersionInfo versionInfo = new VersionInfo("schema2", serializationFormat.getFullTypeName(), 5, 5);
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        CodecType codecType = new CodecType("gzip");
        EncodingInfo encodingInfo = new EncodingInfo(versionInfo, schemaInfo, codecType);
        EncodingId encodingId = new EncodingId(5);
        doReturn(ModelHelper.encode(encodingInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo.class);
        EncodingInfo encodingInfo1 = client.getEncodingInfo("mygroup", encodingId);
        assertEquals(encodingInfo.getCodecType(), encodingInfo1.getCodecType());
        assertEquals(encodingInfo.getSchemaInfo(), encodingInfo1.getSchemaInfo());
        assertEquals(encodingInfo.getVersionInfo(), encodingInfo1.getVersionInfo());
        // NotFound exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup", encodingId), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingInfo("mygroup", encodingId), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetEncodingId() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getEncodingId(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        String codecType = "gzip";
        VersionInfo versionInfo = new VersionInfo("schema2", "a", 5, 5);
        io.pravega.schemaregistry.contract.generated.rest.model.EncodingId encodingId = ModelHelper.encode(new EncodingId(5));
        doReturn(encodingId).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.EncodingId.class);
        EncodingId encodingId1 = client.getEncodingId("mygroup", versionInfo, codecType);
        assertEquals(encodingId.getEncodingId().intValue(), encodingId1.getId());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof ResourceNotFoundException);
        // StringNotFound Exception
        doReturn(Response.Status.PRECONDITION_FAILED.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof CodecTypeNotRegisteredException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getEncodingId("mygroup", versionInfo, codecType), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetLatestSchemaForGroup() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemas(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        VersionInfo versionInfo = new VersionInfo("schema2", serializationFormat.getFullTypeName(), 5, 5);
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        SchemaVersionsList schemaWithVersions = new SchemaVersionsList().schemas(Collections.singletonList(ModelHelper.encode(schemaWithVersion)));
        doReturn(schemaWithVersions).when(response).readEntity(
                SchemaVersionsList.class);
        SchemaWithVersion schemaWithVersion1 = client.getLatestSchemaVersion("mygroup", null);
        assertEquals(schemaWithVersion.getSchemaInfo(), schemaWithVersion1.getSchemaInfo());
        assertEquals(schemaWithVersion.getVersionInfo(), schemaWithVersion1.getVersionInfo());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", null), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", null), e -> e instanceof InternalServerError);

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        serializationFormat = SerializationFormat.custom("custom");
        versionInfo = new VersionInfo("schema2", serializationFormat.getFullTypeName(), 5, 5);
        schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        doReturn(ModelHelper.encode(schemaWithVersion)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion.class);
        schemaWithVersion1 = client.getLatestSchemaVersion("mygroup", "myobject");
        assertEquals(schemaWithVersion.getSchemaInfo(), schemaWithVersion1.getSchemaInfo());
        assertEquals(schemaWithVersion.getVersionInfo(), schemaWithVersion1.getVersionInfo());
        // NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", "myobject"), e -> e instanceof ResourceNotFoundException);
        // Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getLatestSchemaVersion("mygroup", "myobject"), e -> e instanceof InternalServerError);
    }
    
    @Test
    public void testGroupEvolutionHistory() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getGroupHistory(any(), anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        VersionInfo versionInfo = new VersionInfo("schema2", serializationFormat.getFullTypeName(), 5, 5);
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        Compatibility compatibility = Compatibility.backward();
        GroupHistoryRecord groupHistoryRecord = new io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord()
                .schemaInfo(ModelHelper.encode(schemaInfo)).versionInfo(ModelHelper.encode(versionInfo))
                .compatibility(ModelHelper.encode(compatibility)).timestamp(100L).schemaString("");
        GroupHistory history = new GroupHistory();
        history.addHistoryItem(groupHistoryRecord);
        doReturn(history).when(response).readEntity(GroupHistory.class);
        List<io.pravega.schemaregistry.contract.data.GroupHistoryRecord> groupHistoryList = client.getGroupHistory("mygroup");
        assertEquals(1, groupHistoryList.size());
        assertEquals(compatibility, groupHistoryList.get(0).getCompatibility());
        assertEquals(schemaInfo, groupHistoryList.get(0).getSchemaInfo());
        assertEquals(versionInfo, groupHistoryList.get(0).getVersionInfo());
        assertEquals(100L, groupHistoryList.get(0).getTimestamp());
        assertEquals("", groupHistoryList.get(0).getSchemaString());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupHistory("mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getGroupHistory("mygroup"), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetSchemaVersion() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaVersion(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);
        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("schema2", schemaInfo.getSerializationFormat().getFullTypeName(), 5, 5);
        doReturn(ModelHelper.encode(versionInfo)).when(response).readEntity(
                io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo.class);
        VersionInfo versionInfo1 = client.getVersionForSchema("mygroup", schemaInfo);
        assertEquals(versionInfo.getType(), versionInfo1.getType());
        assertEquals(versionInfo.getVersion(), versionInfo1.getVersion());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getVersionForSchema("mygroup", schemaInfo), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getVersionForSchema("mygroup", schemaInfo), e -> e instanceof InternalServerError);
    }
    
    @Test
    public void testGetSchemaVersions() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getSchemaVersions(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);

        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
        VersionInfo versionInfo = new VersionInfo("schema2", schemaInfo.getSerializationFormat().getFullTypeName(), 5, 5);
        SchemaWithVersion schemaWithVersion = new SchemaWithVersion(schemaInfo, versionInfo);
        SchemaVersionsList list = new SchemaVersionsList().schemas(Collections.singletonList(ModelHelper.encode(schemaWithVersion)));
        doReturn(list).when(response).readEntity(SchemaVersionsList.class);
        List<SchemaWithVersion> result = Lists.newArrayList(client.getSchemaVersions("mygroup", null));
        assertEquals(result.size(), 1);
        assertEquals(versionInfo, result.get(0).getVersionInfo());
        assertEquals(schemaInfo, result.get(0).getSchemaInfo());

        result = Lists.newArrayList(client.getSchemaVersions("mygroup", schemaInfo.getType()));
        assertEquals(result.size(), 1);
        assertEquals(versionInfo, result.get(0).getVersionInfo());
        assertEquals(schemaInfo, result.get(0).getSchemaInfo());
        
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> Lists.newArrayList(client.getSchemaVersions("mygroup", null)), 
                e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> Lists.newArrayList(client.getSchemaVersions("mygroup", null)), e -> e instanceof InternalServerError);
    }

    @Test
    public void testValidateSchema() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).validate(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);

        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
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
                () -> client.validateSchema("mygroup", schemaInfo), e -> e instanceof InternalServerError);
    }

    @Test
    public void testCanRead() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).canRead(any(), anyString(), any());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        SerializationFormat serializationFormat = SerializationFormat.custom("custom");
        ByteBuffer schemaData = ByteBuffer.wrap(new byte[0]);

        SchemaInfo schemaInfo = new SchemaInfo("schema1", serializationFormat, schemaData, ImmutableMap.of());
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
                () -> client.canReadUsing("mygroup", schemaInfo), e -> e instanceof InternalServerError);
    }

    @Test
    public void testGetCodecTypes() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).getCodecTypesList(any(), anyString());

        doReturn(Response.Status.OK.getStatusCode()).when(response).getStatus();
        CodecType codecType = new CodecType("gzip");
        CodecType codecType1 = new CodecType("snappy");
        CodecTypes codecTypesList = new CodecTypes();
        codecTypesList.addCodecTypesItem(ModelHelper.encode(codecType));
        codecTypesList.addCodecTypesItem(ModelHelper.encode(codecType1));
        doReturn(codecTypesList).when(response).readEntity(CodecTypes.class);
        List<CodecType> codecTypesList1 = client.getCodecTypes("mygroup");
        assertEquals(2, codecTypesList1.size());
        assertEquals("gzip", codecTypesList1.get(0).getName());
        assertEquals("snappy", codecTypesList1.get(1).getName());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecTypes("mygroup"), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.getCodecTypes("mygroup"), e -> e instanceof InternalServerError);
    }

    @Test
    public void testAddCodecType() {
        ApiV1.GroupsApi proxy = mock(ApiV1.GroupsApi.class);
        SchemaRegistryClientImpl client = new SchemaRegistryClientImpl(proxy);
        Response response = mock(Response.class);
        doReturn(response).when(proxy).addCodecType(any(), anyString(), any());

        doReturn(Response.Status.CREATED.getStatusCode()).when(response).getStatus();
        CodecType codecType = new CodecType("gzip");
        client.addCodecType("mygroup", codecType);
        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        //NotFound Exception
        doReturn(Response.Status.NOT_FOUND.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodecType("mygroup", codecType), e -> e instanceof ResourceNotFoundException);
        //Runtime Exception
        doReturn(Response.Status.CONFLICT.getStatusCode()).when(response).getStatus();
        AssertExtensions.assertThrows("An exception should have been thrown",
                () -> client.addCodecType("mygroup", codecType), e -> e instanceof InternalServerError);
    }
}
