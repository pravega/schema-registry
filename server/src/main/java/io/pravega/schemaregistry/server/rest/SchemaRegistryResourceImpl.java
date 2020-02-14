/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import io.pravega.schemaregistry.contract.SchemaRegistryContract.SchemaValidationRules;
import io.pravega.schemaregistry.exceptions.EntityExistsException;
import io.pravega.schemaregistry.server.rest.generated.api.NotFoundException;
import io.pravega.schemaregistry.server.rest.generated.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CompressionsList;
import io.pravega.schemaregistry.server.rest.generated.model.CreateGroupRequest;
import io.pravega.schemaregistry.server.rest.generated.model.CreateScopeRequest;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingId;
import io.pravega.schemaregistry.server.rest.generated.model.EncodingInfo;
import io.pravega.schemaregistry.server.rest.generated.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.server.rest.generated.model.GroupsList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaEvolutionList;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaWithVersion;
import io.pravega.schemaregistry.server.rest.generated.model.ScopeProperty;
import io.pravega.schemaregistry.server.rest.generated.model.ScopesList;
import io.pravega.schemaregistry.server.rest.generated.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.server.rest.generated.model.ValidateRequest;
import io.pravega.schemaregistry.server.rest.generated.model.VersionInfo;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.concurrent.CompletionException;

import static javax.ws.rs.core.Response.Status;
import static javax.ws.rs.core.Response.status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaRegistryResourceImpl implements ApiV1.ScopesApi {

    @Context
    HttpHeaders headers;

    private SchemaRegistryService registryService;
    
    public SchemaRegistryResourceImpl(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    @Override
    public void listScopes(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.listScopes()
                         .thenApply(scopesList -> {
                             ScopesList scopes = new ScopesList();
                             scopesList.forEach(x -> {
                                 scopes.addScopesItem(new ScopeProperty().scopeName(x));
                             });
                             return Response.status(Status.OK).entity(scopes).build(); })
                         .exceptionally(exception -> {
                             log.warn("listScopes failed with exception: ", exception);
                             return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                         .thenApply(response -> {
                             asyncResponse.resume(response);
                             return response;
                         });
    }

    @Override
    public void createScope(CreateScopeRequest createScopeRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        try {
            NameUtils.validateUserScopeName(createScopeRequest.getScopeName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create scope failed due to invalid scope name {}", createScopeRequest.getScopeName());
            asyncResponse.resume(status(Status.BAD_REQUEST).build());
            return;
        }

        registryService.createScope(createScopeRequest.getScopeName()).thenApply(r -> {
            log.info("Successfully created new scope: {}", createScopeRequest.getScopeName());
            return status(Status.CREATED).entity(new ScopeProperty().scopeName(createScopeRequest.getScopeName())).build();
        }).exceptionally(e -> {
            if (Exceptions.unwrap(e) instanceof EntityExistsException) {
                log.warn("Scope name: {} already exists", createScopeRequest.getScopeName());
                return status(Status.CONFLICT).build();
            } else {
                log.warn("Failed to create scope: {}", createScopeRequest.getScopeName());
                return status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("createScope for scope: {} failed, exception: {}", createScopeRequest.getScopeName(), exception);
            return status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume);
    }

    @Override
    public void deleteScope(String scopeName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteScope(scopeName)
                       .thenApply(status -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void listGroups(String scopeName, SecurityContext securityContext,
                           AsyncResponse asyncResponse) throws NotFoundException {
        registryService.listGroupsInScope(scopeName)
                       .thenApply(groupsInScope -> {
                           GroupsList groupsList = new GroupsList();
                           groupsInScope.forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
                           return Response.status(Status.OK).entity(groupsList).build(); })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof NotFoundException) {
                               
                           }
                           log.warn("listScopes failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void createGroup(String scopeName, CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        SchemaRegistryContract.GroupProperties properties = new SchemaRegistryContract.GroupProperties(
                schemaType, validationRules, createGroupRequest.getGroupByEventType(), createGroupRequest.getEnableEncoding());
        registryService.createGroup(scopeName, createGroupRequest.getGroupName(), properties)
                       .thenApply(createStatus -> {
                           if (!createStatus) {
                               throw new CompletionException(new EntityExistsException());
                           }
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("createGroup failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getGroupProperties(String scopeName, String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupProperties(scopeName, groupName)
                       .thenApply(groupProperty -> Response.status(Status.OK).entity(ModelHelper.encode(groupProperty)).build())
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void updateSchemaValidationRules(String scopeName, String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
        registryService.updateSchemaValidationPolicy(scopeName, groupName, rules)
                       .thenApply(groupProperty -> Response.status(Status.OK).build())
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void deleteGroup(String scopeName, String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteGroup(scopeName, groupName)
                       .thenApply(status -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getGroupSchemas(String scopeName, String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(scopeName, groupName, null)
                       .thenApply(schemasEvolutionList -> {
                           SchemaEvolutionList list = ModelHelper.encode(schemasEvolutionList);
                           return Response.status(Status.OK).entity(list).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getLatestGroupSchema(String scopeName, String groupName, SecurityContext securityContext,
                                     AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(scopeName, groupName, null)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void addSchemaToGroupIfAbsent(String scopeName, String groupName, AddSchemaToGroupRequest addSchemaRequest, 
                                             SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());
        SchemaValidationRules schemaValidationRules = ModelHelper.decode(addSchemaRequest.getRules());

        registryService.addSchemaIfAbsent(scopeName, groupName, schemaInfo, schemaValidationRules)
                       .thenApply(versionInfo -> {
                           VersionInfo version = ModelHelper.encode(versionInfo);
                           return Response.status(Status.OK).entity(version).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void validate(String scopeName, String groupName, ValidateRequest validateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
        SchemaValidationRules rules = ModelHelper.decode(validateRequest.getValidationRules());
        registryService.validateSchema(scopeName, groupName, schemaInfo, rules)
                       .thenApply(compatible -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaFromVersion(String scopeName, String groupName, GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.VersionInfo versionInfo = ModelHelper.decode(getSchemaFromVersionRequest.getVersionInfo());
        registryService.getSchema(scopeName, groupName, versionInfo)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getOrGenerateEncodingId(String scopeName, String groupName, GetEncodingIdRequest getEncodingIdRequest,
                                        SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        SchemaRegistryContract.CompressionType compressionType = ModelHelper.decode(getEncodingIdRequest.getCompressionType());
        registryService.getEncodingId(scopeName, groupName, version, compressionType)
                       .thenApply(encodingId -> {
                           EncodingId id = ModelHelper.encode(encodingId);
                           return Response.status(Status.OK).entity(id).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getEncodingInfo(String scopeName, String groupName, Integer encodingId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaRegistryContract.EncodingId id = new SchemaRegistryContract.EncodingId(encodingId);
        registryService.getEncodingInfo(scopeName, groupName, id)
                       .thenApply(encodingInfo -> {
                           EncodingInfo encoding = ModelHelper.encode(encodingInfo);
                           return Response.status(Status.OK).entity(encoding).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }


    @Override
    public void getCompressionsList(String scopeName, String groupName, SecurityContext securityContext,
                                    AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getCompressions(scopeName, groupName)
                       .thenApply(list -> {
                           CompressionsList compressionsList = ModelHelper.encodeCompressionList(list);
                           return Response.status(Status.OK).entity(compressionsList).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getLatestSubgroupSchema(String scopeName, String groupName, String subgroupName, 
                                            SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(scopeName, groupName, subgroupName)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getSubGroupSchemas(String scopeName, String groupName, String subgroupName, SecurityContext securityContext, 
                                       AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(scopeName, groupName, subgroupName)
                       .thenApply(schemasWithVersions -> {
                           SchemaEvolutionList list = ModelHelper.encode(schemasWithVersions);
                           return Response.status(Status.OK).entity(list).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
}
