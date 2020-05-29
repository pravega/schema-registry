/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.schemaregistry.storage.impl.group;

import io.pravega.client.ClientConfig;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.pravegastandalone.PravegaStandaloneUtils;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.TableKeySerializer;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import io.pravega.schemaregistry.storage.impl.groups.GroupsValue;
import io.pravega.schemaregistry.storage.impl.groups.PravegaKVGroups;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class GroupPravegaTest {
    private static final TableKeySerializer KEY_SERIALIZER = new TableKeySerializer();
    private ScheduledExecutorService executor;
    String groupName;
    ClientConfig clientConfig;
    PravegaKVGroupTable pravegaKVGroupTable;
    TableStore tableStore;
    GrpcAuthHelper grpcAuthHelper;
    Group<Version> group;
    PravegaKVGroups pravegaKVGroups;
    static final String GROUPS = "schema-registry/groups/0";

    @Before
    public void setUp() {
        grpcAuthHelper = new GrpcAuthHelper(Boolean.FALSE, null, null);
        PravegaStandaloneUtils pravegaStandaloneUtils = PravegaStandaloneUtils.startPravega();
        clientConfig = ClientConfig.builder().controllerURI(
                URI.create(pravegaStandaloneUtils.getControllerURI())).build();
        groupName = "mygroup";
        executor = Executors.newScheduledThreadPool(5);
        tableStore = new TableStore(clientConfig, () -> "", executor);
        //pravegaKVGroupTable = new PravegaKVGroupTable(groupName, "tableStore", tableStore);
        pravegaKVGroups = new PravegaKVGroups(tableStore, executor);
        //group = new Group<>(groupName, pravegaKVGroupTable,executor);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testCreate() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        assertTrue(tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord().getState().equals(GroupsValue.State.Active));
    }

    @Test
    public void testGetCurrentEtag() {
        GroupProperties groupProperties = new GroupProperties(SchemaType.custom("custom1"),
                SchemaValidationRules.of(Compatibility.backward()), Boolean.TRUE,
                Collections.singletonMap("key", "value"));
        pravegaKVGroups.addNewGroup(groupName, groupProperties).join();
        pravegaKVGroups.getGroup(groupName).join().create(SchemaType.Custom, Collections.singletonMap("key", "value"),
                Boolean.TRUE, SchemaValidationRules.of(Compatibility.backward())).join();
        Etag etag = pravegaKVGroups.getGroup(groupName).join().getCurrentEtag().join();
        assertTrue(tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord().getState().equals(GroupsValue.State.Active));
        GroupsValue gv = tableStore.getEntry(GROUPS, groupName.getBytes(Charsets.UTF_8),
                GroupsValue::fromBytes).join().getRecord();
        TableRecords.Etag etag1 = tableStore.getEntry(String.format("table-%s/metadata/0", gv.getId()), new TableRecords.Etag().toBytes(),
               x -> TableRecords.fromBytes(TableRecords.Etag.class, x, TableRecords.Etag.class)).join().getRecord();
        /*List<Version> versionList = tableStore.getAllEntries(String.format("table-%s/metadata/0", gv.getId()), x -> x, x -> x)
                .thenApply(entries -> entries.stream().filter(x -> KEY_SERIALIZER.fromBytes(x.getKey()) instanceof TableRecords.Etag).map(
                        x -> {
                            Version version = x.getValue().getVersion();
                            return version;
                        }).collect(Collectors.toList())).join();
        assertEquals(1, versionList.size());*/
        //tableStore.getAllEntries(String.format("table-%s/metadata/0", gv.getId()), x -> x, x -> x).join().stream()
       // assertEquals(etag.etag(), (tableStore.getEntry(gv.getId(), new TableRecords.Etag().toBytes(),
         //       x -> TableRecords.fromBytes(TableRecords.Etag.class, x, TableRecords.Etag.class)).join().getVersion().getVersion()));
    }



}
















