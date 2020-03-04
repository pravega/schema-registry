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
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaValidationRuleRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EventTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaForEventTypeByVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CompressionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.v1.ApiV1;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

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
public class SchemaRegistryResourceImpl implements ApiV1.GroupsApi {

    @Context
    HttpHeaders headers;

    private SchemaRegistryService registryService;
    
    public SchemaRegistryResourceImpl(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    @Override
    public void listGroups(SecurityContext securityContext,
                           AsyncResponse asyncResponse) throws NotFoundException {
        registryService.listGroups(null)
                       .thenApply(groups -> {
                           GroupsList groupsList = new GroupsList();
                           groups.getMap().forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
                           return Response.status(Status.OK).entity(groupsList).build(); })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof NotFoundException) {
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("listGroups failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void createGroup(CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        GroupProperties properties = new GroupProperties(
                schemaType, validationRules, createGroupRequest.isGroupByEventType(), createGroupRequest.isEnableEncoding());
        registryService.createGroup(createGroupRequest.getGroupName(), properties)
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
    public void getGroupProperties(String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupProperties(groupName)
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
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
        registryService.updateSchemaValidationPolicy(groupName, rules)
                       .thenApply(groupProperty -> Response.status(Status.OK).build())
                       .exceptionally(exception -> {
                           log.warn("updateSchemaValidationRules failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaValidationRules(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        throw new NotImplementedException("get schema validation rules");
    }

    @Override
    public void getSchemaValidationRule(String groupName, String rule, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        throw new NotImplementedException("get schema validation rule");
    }

    @Override
    public void deleteSchemaValidationRule(String groupName, String rule, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        throw new NotImplementedException("delete schema validation rule");
    }

    @Override
    public void addSchemaValidationRule(String groupName, AddSchemaValidationRuleRequest addSchemaValidationRuleRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        throw new NotImplementedException("add schema validation rule");
    }

    @Override
    public void deleteGroup(String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteGroup(groupName)
                       .thenApply(status -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("deleteGroup failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getGroupSchemas(String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(groupName, null)
                       .thenApply(schemasEvolutionList -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemasEvolutionList.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(list).build(); })
                       .exceptionally(exception -> {
                           log.warn("getGroupSchemas failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getLatestGroupSchema(String groupName, SecurityContext securityContext,
                                     AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(groupName, null)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getLatestGroupSchema failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void addSchemaToGroupIfAbsent(String groupName, AddSchemaToGroupRequest addSchemaRequest, 
                                             SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());
        SchemaValidationRules schemaValidationRules = ModelHelper.decode(addSchemaRequest.getRules());

        registryService.addSchemaIfAbsent(groupName, schemaInfo, schemaValidationRules)
                       .thenApply(versionInfo -> {
                           VersionInfo version = ModelHelper.encode(versionInfo);
                           return Response.status(Status.OK).entity(version).build(); })
                       .exceptionally(exception -> {
                           log.warn("addSchemaToGroupIfAbsent failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void validate(String groupName, ValidateRequest validateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
        SchemaValidationRules rules = ModelHelper.decode(validateRequest.getValidationRules());
        registryService.validateSchema(groupName, schemaInfo, rules)
                       .thenApply(compatible -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("validate failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void canRead(String groupName, CanReadRequest canReadRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(canReadRequest.getSchemaInfo());
        registryService.canRead(groupName, schemaInfo)
                       .thenApply(compatible -> {
                           return Response.status(Status.OK).build(); })
                       .exceptionally(exception -> {
                           log.warn("can read failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getSchemaFromVersion(String groupName, String versionId, GetSchemaFromVersionRequest getSchemaFromVersionRequest,
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(getSchemaFromVersionRequest.getVersionInfo());
        registryService.getSchema(groupName, versionInfo)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getSchemaFromVersion failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getOrGenerateEncodingId(String groupName, GetEncodingIdRequest getEncodingIdRequest,
                                        SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        CompressionType compressionType = ModelHelper.decode(getEncodingIdRequest.getCompressionType());
        registryService.getEncodingId(groupName, version, compressionType)
                       .thenApply(encodingId -> {
                           EncodingId id = ModelHelper.encode(encodingId);
                           return Response.status(Status.OK).entity(id).build(); })
                       .exceptionally(exception -> {
                           log.warn("getOrGenerateEncodingId failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaVersion(String groupName, GetSchemaVersion getSchemaVersion, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(getSchemaVersion.getSchemaInfo());

        registryService.getSchemaVersion(groupName, schemaInfo)
                       .thenApply(version -> {
                           VersionInfo versionInfo = ModelHelper.encode(version);
                           return Response.status(Status.OK).entity(versionInfo).build(); })
                       .exceptionally(exception -> {
                           log.warn("getSchemaVersion failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getEventTypeSchemas(String groupName, String eventTypeName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getGroupEvolutionHistory(groupName, eventTypeName)
                       .thenApply(schemaEpochs -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemaEpochs.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(list).build(); })
                       .exceptionally(exception -> {
                           log.warn("getEventTypeSchemas failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getEventTypes(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getEventTypes(groupName, null)
                       .thenApply(eventTypes -> {
                           EventTypesList eventTypesList = new EventTypesList().groups(eventTypes.getList());
                           return Response.status(Status.OK).entity(eventTypesList).build(); })
                       .exceptionally(exception -> {
                           log.warn("getEventTypes failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });

    }

    @Override
    public void getLatestSchemaForEventType(String groupName, String eventTypeName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getLatestSchema(groupName, eventTypeName)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getLatestSchemaForEventType failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaFromVersionForEventType(String groupName, String eventType, Integer versionId,
                                             GetSchemaForEventTypeByVersionRequest getSchemaForEventTypeByVersionRequest, 
                                             SecurityContext securityContext, AsyncResponse asyncResponse) 
            throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(
                getSchemaForEventTypeByVersionRequest.getVersionInfo());
        registryService.getSchema(groupName, versionInfo)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build(); })
                       .exceptionally(exception -> {
                           log.warn("getSchemaFromVersionForEventType failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getEncodingInfo(String groupName, Integer encodingId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
        registryService.getEncodingInfo(groupName, id)
                       .thenApply(encodingInfo -> {
                           EncodingInfo encoding = ModelHelper.encode(encodingInfo);
                           return Response.status(Status.OK).entity(encoding).build(); })
                       .exceptionally(exception -> {
                           log.warn("getEncodingInfo failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }


    @Override
    public void getCompressionsList(String groupName, SecurityContext securityContext,
                                    AsyncResponse asyncResponse) throws NotFoundException {
        registryService.getCompressions(groupName)
                       .thenApply(list -> {
                           CompressionsList compressionsList = new CompressionsList()
                                   .compressionTypes(list.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(compressionsList).build(); })
                       .exceptionally(exception -> {
                           log.warn("getCompressionsList failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build(); })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
}
