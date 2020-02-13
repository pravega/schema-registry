/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import io.pravega.schemaregistry.contract.SchemaValidationRules;
import io.pravega.schemaregistry.server.rest.generated.api.NotFoundException;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToSubgroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CanReadUsingSchemaRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CheckCompatibilityRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CreateGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CreateScopeRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.server.rest.generated.model.UpdateCompatibilityPolicyRequest;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaRegistryResourceImpl implements ApiV1.ScopesApi {

    @Context
    HttpHeaders headers;

    private ConnectionFactory connectionFactory;
    private SchemaRegistryService registryService;
    
    public SchemaRegistryResourceImpl(ConnectionFactory connectionFactory, SchemaRegistryService registryService) {
        this.connectionFactory = connectionFactory;
        this.registryService = registryService;
    }

    @Override
    public void listScopes(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void createScope(CreateScopeRequest createScopeRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void listGroups(String scopeName, String ERROR_UNKNOWN, SecurityContext securityContext,
                           AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void createGroup(String scopeName, CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getGroupProperties(String scopeName, String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void updateCompatibilityPolicy(String scopeName, String groupName, UpdateCompatibilityPolicyRequest updateCompatibilityPolicyRequest,
                                          SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void deleteGroup(String scopeName, String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getGroupSchemas(String scopeName, String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getLatestGroupSchema(String scopeName, String groupName, SecurityContext securityContext,
                                     AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void addSchemaToGroupIfAbsent(String scopeName, String groupName, AddSchemaToGroupRequest addSchemaRequest, 
                                             SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());
        SchemaValidationRules schemaValidationRules = ModelHelper.decode(addSchemaRequest.getRules());
        
        registryService.addSchemaIfAbsent(scopeName, groupName, null, schemaInfo, schemaValidationRules)
            .thenRun(() -> {
            });               
    }

    @Override
    public void addSchemaToSubgroupIfAbsent(String scopeName, String groupName, String subgroupName,
                                            AddSchemaToSubgroupRequest addSchemaRequest, SecurityContext securityContext,
                                            AsyncResponse asyncResponse) throws NotFoundException {
        
    }
    
    @Override
    public void getSchemaFromVersion(String scopeName, String groupName, GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getEncodingId(String scopeName, String groupName, String encodingId, SecurityContext securityContext,
                              AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getOrGenerateEncodingId(String scopeName, String groupName, GetEncodingIdRequest getEncodingIdRequest,
                                        SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void getCompressionsList(String scopeName, String groupName, SecurityContext securityContext,
                                    AsyncResponse asyncResponse) throws NotFoundException {

    }
    
    @Override
    public void getLatestSubgroupSchema(String scopeName, String groupName, String subgroupName, 
                                            SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        
    }

    @Override
    public void getSchemaFromSubgroupVersion(String scopeName, String groupName, String subgroupName, 
                                                 GetSchemaFromVersionRequest getSchemaFromVersionRequest, SecurityContext securityContext, 
                                                 AsyncResponse asyncResponse) throws NotFoundException {
        
    }

    @Override
    public void getSubGroupSchemas(String scopeName, String groupName, String subgroupName, SecurityContext securityContext, 
                                       AsyncResponse asyncResponse) throws NotFoundException {
        
    }


    @Override
    public void canRead(String scopeName, String groupName, CanReadUsingSchemaRequest canReadUsingSchemaRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }

    @Override
    public void checkCompatibility(String scopeName, String groupName, CheckCompatibilityRequest checkCompatibilityRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {

    }
}
