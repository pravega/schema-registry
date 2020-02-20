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

import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespaceProperty;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateNamespaceRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaEvolutionList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.NamespacesList;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.exceptions.EntityExistsException;
import io.pravega.schemaregistry.server.rest.v1.ApiV1;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status;
import static javax.ws.rs.core.Response.status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaRegistryResourceImpl implements ApiV1.NamespacesApi {

    @Context
    HttpHeaders headers;

    private SchemaRegistryService registryService;
    
    public SchemaRegistryResourceImpl(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    @Override
    public void listNamespaces(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.listNamespaces(null)
                         .thenApply(namespacesList -> {
                             NamespacesList namespaces = new NamespacesList();
                             namespacesList.getList().forEach(x -> {
                                 namespaces.addNamespacesItem(new NamespaceProperty().namespaceName(x));
                             });
                             return Response.status(Status.OK).entity(namespaces).build(); })
                         .exceptionally(exception -> {
                             log.warn("listNamespaces failed with exception: ", exception);
                             return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                         .thenApply(response -> {
                             asyncResponse.resume(response);
                             return response;
                         });
    }

    @Override
    public void createNamespace(CreateNamespaceRequest createNamespaceRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        try {
            NameUtils.validateUserScopeName(createNamespaceRequest.getNamespaceName());
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn("Create namespace failed due to invalid namespace name {}", createNamespaceRequest.getNamespaceName());
            asyncResponse.resume(status(Status.BAD_REQUEST).build());
            return;
        }

        registryService.createNamespace(createNamespaceRequest.getNamespaceName()).thenApply(r -> {
            log.info("Successfully created new namespace: {}", createNamespaceRequest.getNamespaceName());
            return status(Status.CREATED).entity(new NamespaceProperty().namespaceName(createNamespaceRequest.getNamespaceName())).build();
        }).exceptionally(e -> {
            if (Exceptions.unwrap(e) instanceof EntityExistsException) {
                log.warn("Namespace name: {} already exists", createNamespaceRequest.getNamespaceName());
                return status(Status.CONFLICT).build();
            } else {
                log.warn("Failed to create namespace: {}", createNamespaceRequest.getNamespaceName());
                return status(Status.INTERNAL_SERVER_ERROR).build();
            }
        }).exceptionally(exception -> {
            log.warn("createNamespace for namespace: {} failed, exception: {}", createNamespaceRequest.getNamespaceName(), exception);
            return status(Status.INTERNAL_SERVER_ERROR).build();
        }).thenApply(asyncResponse::resume);
    }

    @Override
    public void deleteNamespace(String namespaceName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteNamespace(namespaceName)
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
    public void listGroups(String namespaceName, SecurityContext securityContext,
                           AsyncResponse asyncResponse) throws NotFoundException {
        registryService.listGroupsInNamespace(namespaceName, null)
                       .thenApply(groupsInNamespace -> {
                           GroupsList groupsList = new GroupsList();
                           groupsInNamespace.getMap().forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
                           return Response.status(Status.OK).entity(groupsList).build(); })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof NotFoundException) {
                               
                           }
                           log.warn("listNamespaces failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void createGroup(String namespaceName, CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        GroupProperties properties = new GroupProperties(
                schemaType, validationRules, createGroupRequest.isGroupByEventType(), createGroupRequest.isEnableEncoding());
        registryService.createGroup(namespaceName, createGroupRequest.getGroupName(), properties)
                       .thenApply(createStatus -> {
                           if (!createStatus) {
                               return Response.status(Status.CONFLICT).build();
                           }
                           return Response.status(Status.OK).build(); 
                       })
                       .exceptionally(exception -> {
                           log.warn("createGroup failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getGroupProperties(String namespaceName, String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupProperties(namespaceName, groupName)
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
    public void updateSchemaValidationRules(String namespaceName, String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
        registryService.updateSchemaValidationPolicy(namespaceName, groupName, rules)
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
    public void deleteGroup(String namespaceName, String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteGroup(namespaceName, groupName)
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
    public void getGroupSchemas(String namespaceName, String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(namespaceName, groupName, null)
                       .thenApply(schemasEvolutionList -> {
                           SchemaEvolutionList list = new SchemaEvolutionList()
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
    public void getLatestGroupSchema(String namespaceName, String groupName, SecurityContext securityContext,
                                     AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(namespaceName, groupName, null)
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
    public void addSchemaToGroupIfAbsent(String namespaceName, String groupName, AddSchemaToGroupRequest addSchemaRequest, 
                                             SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());
        SchemaValidationRules schemaValidationRules = ModelHelper.decode(addSchemaRequest.getRules());

        registryService.addSchemaIfAbsent(namespaceName, groupName, schemaInfo, schemaValidationRules)
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
    public void validate(String namespaceName, String groupName, ValidateRequest validateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
        SchemaValidationRules rules = ModelHelper.decode(validateRequest.getValidationRules());
        registryService.validateSchema(namespaceName, groupName, schemaInfo, rules)
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
    public void getSchemaFromVersion(String namespaceName, String groupName, GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(getSchemaFromVersionRequest.getVersionInfo());
        registryService.getSchema(namespaceName, groupName, versionInfo)
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
    public void getOrGenerateEncodingId(String namespaceName, String groupName, GetEncodingIdRequest getEncodingIdRequest,
                                        SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        CompressionType compressionType = ModelHelper.decode(getEncodingIdRequest.getCompressionType());
        registryService.getEncodingId(namespaceName, groupName, version, compressionType)
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
    public void getEncodingInfo(String namespaceName, String groupName, Integer encodingId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
        registryService.getEncodingInfo(namespaceName, groupName, id)
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
    public void getCompressionsList(String namespaceName, String groupName, SecurityContext securityContext,
                                    AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getCompressions(namespaceName, groupName)
                       .thenApply(list -> {
                           CompressionsList compressionsList = new CompressionsList()
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
    public void getLatestSubgroupSchema(String namespaceName, String groupName, String subgroupName, 
                                            SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(namespaceName, groupName, subgroupName)
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
    public void getSubGroupSchemas(String namespaceName, String groupName, String subgroupName, SecurityContext securityContext, 
                                       AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(namespaceName, groupName, subgroupName)
                       .thenApply(schemaEpochs -> {
                           SchemaEvolutionList list = new SchemaEvolutionList()
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
