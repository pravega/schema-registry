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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.common.AuthHelper;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.RegistryApplication;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.server.rest.filter.AuthenticationFilter;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.After;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SchemaRegistryAuthTest extends JerseyTest {
    private final static String SYSTEM_ADMIN = "admin";
    private final static String SYSTEM_READER = "rootreader";
    private final static String NO_PREMISSION = "unauthenticated";
    private final static String GROUP1_ADMIN = "group1readupdate";
    private final static String GROUP1_USER = "group1read";
    private final static String GROUP2_USER = "group2read";
    private final static String GROUP_1_2_USER = "group1-2read";
    private final static String NAMESPACE_USER = "namespaceuser";
    private final static String NAMESPACE_ADMIN = "namespaceadmin";
    private final static String PASSWORD = "1111_aaaa";

    private SchemaRegistryService service;
    private File authFile;
    private ScheduledExecutorService executor;
    @Override
    protected Application configure() {
        executor = Executors.newSingleThreadScheduledExecutor();
        forceSet(TestProperties.CONTAINER_PORT, "0");
        this.authFile = createAuthFile();

        service = mock(SchemaRegistryService.class);
        final Set<Object> resourceObjs = new HashSet<>();
        ServiceConfig config = ServiceConfig.builder().authEnabled(true).userPasswordFilePath(authFile.getAbsolutePath()).build();
        AuthHandlerManager authHandlerManager = new AuthHandlerManager(config);
        resourceObjs.add(new AuthenticationFilter(config.isAuthEnabled(), authHandlerManager));
        resourceObjs.add(new GroupResourceImpl(service, config, authHandlerManager, executor));
        resourceObjs.add(new SchemaResourceImpl(service, config, authHandlerManager, executor));

        RegistryApplication registryApplication = new RegistryApplication(resourceObjs);
        return registryApplication;
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
        authFile.delete();
    }

    @Test(timeout = 10000)
    public void groups() throws ExecutionException, InterruptedException {
        GroupProperties group1 = new GroupProperties(SerializationFormat.Avro,
                Compatibility.backward(),
                false);
        doAnswer(x -> {
            return CompletableFuture.completedFuture(new ResultPage<>(Collections.emptyList(), ContinuationToken.fromString("token")));
        }).when(service).listGroups(any(), any(), anyInt());

        doAnswer(x -> {
            Map<String, GroupProperties> map = new HashMap<>();
            for (int i = 0; i < 4; i++) {
                map.put("group" + i, group1);
            }
            return CompletableFuture.completedFuture(new ResultPage<>(Lists.newArrayList(map.entrySet()), ContinuationToken.fromString("token")));
        }).when(service).listGroups(any(), any(), eq(10));

        Future<Response> future = target("v1/groups").queryParam("limit", 10)
                                                     .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((SYSTEM_ADMIN + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                                     .async().get();
        Response response = future.get();
        assertEquals(response.getStatus(), 200);
        ListGroupsResponse list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 4);

        future = target("v1/groups").queryParam("limit", 10)
                                    .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((NO_PREMISSION + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                    .async().get();
        response = future.get();
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());

        future = target("v1/groups").queryParam("limit", 10)
                                    .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((GROUP1_ADMIN + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                    .async().get();
        response = future.get();
        assertEquals(response.getStatus(), 200);
        list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 1);

        // only authorized groups are listed. 
        // user Group1-2 is authorized on group1, group2, group11, group12. 
        Map<String, GroupProperties> map1 = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map1.put("group" + i, group1);
        }
        Map<String, GroupProperties> map2 = new HashMap<>();
        for (int i = 10; i < 18; i++) {
            map2.put("group" + i, group1);
        }

        doAnswer(x -> CompletableFuture.completedFuture(new ResultPage<>(Lists.newArrayList(map1.entrySet()), ContinuationToken.fromString("token"))))
                .when(service).listGroups(any(), any(), eq(10));
        doAnswer(x -> CompletableFuture.completedFuture(new ResultPage<>(Lists.newArrayList(map2.entrySet()), ContinuationToken.fromString("token"))))
                .when(service).listGroups(any(), any(), eq(8));

        future = target("v1/groups").queryParam("limit", 10)
                                    .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((GROUP_1_2_USER + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                    .async().get();
        response = future.get();
        assertEquals(response.getStatus(), 200);
        list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 4);
        assertTrue(list.getGroups().keySet().contains("group1"));
        assertTrue(list.getGroups().keySet().contains("group2"));
        assertTrue(list.getGroups().keySet().contains("group11"));
        assertTrue(list.getGroups().keySet().contains("group12"));

        // verify namespace user
        future = target("v1/groups").queryParam("limit", 10).queryParam("namespace", "namespace")
                                    .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((NAMESPACE_USER + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                    .async().get();
        response = future.get();
        assertEquals(response.getStatus(), 200);
        list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 1);

        // verify namespace admin
        future = target("v1/groups").queryParam("limit", 10).queryParam("namespace", "namespace")
                                    .request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((NAMESPACE_ADMIN + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                    .async().get();
        response = future.get();
        assertEquals(response.getStatus(), 200);
        list = response.readEntity(ListGroupsResponse.class);
        assertEquals(list.getGroups().size(), 10);
        assertEquals(list.getContinuationToken(), ContinuationToken.fromString("token").toString());
    }

    @Test
    public void groupSchemas() throws ExecutionException, InterruptedException {
        doAnswer(x -> CompletableFuture.completedFuture(true)).when(service).canRead(any(), any(), any());
        SchemaInfo schemaInfo = new SchemaInfo()
                .type("name")
                .serializationFormat(ModelHelper.encode(SerializationFormat.Avro))
                .schemaData(new byte[0])
                .properties(Collections.emptyMap());
        Future<Response> future = target("v1/groups").path("mygroup").path("schemas/versions/canRead").request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((SYSTEM_ADMIN + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                                     .async().post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON));
        Response response = future.get();
        assertTrue(response.readEntity(CanRead.class).isCompatible());

        future = target("v1/groups").path("mygroup").path("schemas/versions/canRead").request().header(HttpHeaders.AUTHORIZATION,
                        AuthHelper.getAuthorizationHeader("Basic", Base64.getEncoder().encodeToString((GROUP1_USER + ":" + PASSWORD).getBytes(Charsets.UTF_8))))
                                                     .async().post(Entity.entity(schemaInfo, MediaType.APPLICATION_JSON));
        response = future.get();
        assertEquals(response.getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }

    private File createAuthFile() {
        try {
            File authFile = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(authFile.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword(PASSWORD);
                writer.write(credentialsAndAclAsString(SYSTEM_ADMIN, defaultPassword, "*,READ_UPDATE;"));
                writer.write(credentialsAndAclAsString(SYSTEM_READER, defaultPassword, "/*,READ"));
                writer.write(credentialsAndAclAsString(GROUP1_ADMIN, defaultPassword, "/group1,READ_UPDATE"));
                writer.write(credentialsAndAclAsString(GROUP1_USER, defaultPassword, "/group1,READ"));
                writer.write(credentialsAndAclAsString(GROUP2_USER, defaultPassword, "/group2,READ"));
                writer.write(credentialsAndAclAsString(GROUP_1_2_USER, defaultPassword, "/group1,READ;/group2,READ;/group11,READ;/group12,READ"));
                writer.write(credentialsAndAclAsString(NAMESPACE_USER, defaultPassword, "namespace/group1,READ_UPDATE"));
                writer.write(credentialsAndAclAsString(NAMESPACE_ADMIN, defaultPassword, "namespace/*,READ_UPDATE"));
            }
            return authFile;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private String credentialsAndAclAsString(String username, String password, String acl) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username)
                && !Strings.isNullOrEmpty(password)
                && acl != null
                && !acl.startsWith(":"));

        // This will return a string that looks like this:"<username>:<pasword>:acl\n"
        return String.format("%s:%s:%s%n", username, password, acl);
    }
}
