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
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateScopeRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingIdModel;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfoModel;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersionModel;
import io.pravega.schemaregistry.contract.generated.rest.model.ScopePropertyModel;
import io.pravega.schemaregistry.contract.generated.rest.model.ScopesListModel;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfoModel;
import io.pravega.schemaregistry.exceptions.EntityExistsException;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

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
        registryService.listScopes(null)
                         .thenApply(scopesList -> {
                             ScopesListModel scopes = new ScopesListModel();
                             scopesList.getList().forEach(x -> {
                                 scopes.addScopesItem(new ScopePropertyModel().scopeName(x));
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
            return status(Status.CREATED).entity(new ScopePropertyModel().scopeName(createScopeRequest.getScopeName())).build();
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
        registryService.listGroupsInScope(scopeName, null)
                       .thenApply(groupsInScope -> {
                           GroupsListModel groupsList = new GroupsListModel();
                           groupsInScope.getMap().forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
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
        SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        GroupProperties properties = new GroupProperties(
                schemaType, validationRules, createGroupRequest.isGroupByEventType(), createGroupRequest.isEnableEncoding());
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
                           SchemaEvolutionListModel list = new SchemaEvolutionListModel()
                                   .schemas(schemasEvolutionList.stream().map(ModelHelper::encode).collect(Collectors.toList()));
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
                           SchemaWithVersionModel schema = ModelHelper.encode(schemaWithVersion);
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
        SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());
        SchemaValidationRules schemaValidationRules = ModelHelper.decode(addSchemaRequest.getRules());

        registryService.addSchemaIfAbsent(scopeName, groupName, schemaInfo, schemaValidationRules)
                       .thenApply(versionInfo -> {
                           VersionInfoModel version = ModelHelper.encode(versionInfo);
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
        SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
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
        VersionInfo versionInfo = ModelHelper.decode(getSchemaFromVersionRequest.getVersionInfo());
        registryService.getSchema(scopeName, groupName, versionInfo)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfoModel schema = ModelHelper.encode(schemaWithVersion);
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
        VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        CompressionType compressionType = ModelHelper.decode(getEncodingIdRequest.getCompressionType());
        registryService.getEncodingId(scopeName, groupName, version, compressionType)
                       .thenApply(encodingId -> {
                           EncodingIdModel id = ModelHelper.encode(encodingId);
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
        EncodingId id = new EncodingId(encodingId);
        registryService.getEncodingInfo(scopeName, groupName, id)
                       .thenApply(encodingInfo -> {
                           EncodingInfoModel encoding = ModelHelper.encode(encodingInfo);
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
                           CompressionsListModel compressionsList = new CompressionsListModel()
                                   .compressionTypes(list.stream().map(ModelHelper::encode).collect(Collectors.toList()));
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
                           SchemaWithVersionModel schema = ModelHelper.encode(schemaWithVersion);
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
                       .thenApply(schemaEpochs -> {
                           SchemaEvolutionListModel list = new SchemaEvolutionListModel()
                                   .schemas(schemaEpochs.stream().map(ModelHelper::encode).collect(Collectors.toList()));
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
