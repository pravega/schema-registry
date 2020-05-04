/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaToGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupsList;
import io.pravega.schemaregistry.contract.generated.rest.model.ObjectTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.server.rest.v1.ApiV1;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status;

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
        log.info("List Groups called");
        registryService.listGroups(null)
                       .thenApply(groups -> {
                           GroupsList groupsList = new GroupsList();
                           groups.getMap().forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
                           return Response.status(Status.OK).entity(groupsList).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("listGroups failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void createGroup(CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException, UnsupportedEncodingException {
        log.info("Create Group called with params {}", createGroupRequest);
        SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        GroupProperties properties = new GroupProperties(
                schemaType, validationRules, createGroupRequest.isValidateByObjectType(), createGroupRequest.getProperties());
        String groupName = URLDecoder.decode(createGroupRequest.getGroupName(), StandardCharsets.UTF_8.toString());
        registryService.createGroup(groupName, properties)
                       .thenApply(createStatus -> {
                           if (!createStatus) {
                               log.info("group {} exists", groupName);
                               return Response.status(Status.CONFLICT).build();
                           }
                           log.info("group {} created", groupName);
                           return Response.status(Status.CREATED).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("createGroup failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getGroupProperties(String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group properties called for group {}", groupName);
        registryService.getGroupProperties(groupName)
                       .thenApply(groupProperty -> {
                           log.info("Group {} property found are {}", groupName, groupProperty);
                           return Response.status(Status.OK).entity(ModelHelper.encode(groupProperty)).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getGroupProperties for group {} failed with exception: {}", groupName, exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Update schema validation rules called for group {} with new request {}", groupName, updateValidationRulesPolicyRequest);
        SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
        SchemaValidationRules previousRules = updateValidationRulesPolicyRequest.getPreviousRules() == null ?
                null : ModelHelper.decode(updateValidationRulesPolicyRequest.getPreviousRules());
        registryService.updateSchemaValidationRules(groupName, rules, previousRules)
                       .thenApply(groupProperty -> Response.status(Status.OK).build())
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           } else if (Exceptions.unwrap(exception) instanceof PreconditionFailedException) {
                               log.warn("updateSchemaValidationRules write conflict {}", groupName);
                               return Response.status(Status.PRECONDITION_REQUIRED).build();
                           } else {
                               log.warn("updateSchemaValidationRules failed with exception: ", exception);
                               return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                           }
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaValidationRules(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group schema validation rules called for group {}", groupName);
        registryService.getGroupProperties(groupName)
                       .thenApply(groupProperty -> {
                           log.info("Group {} validation rules found are {}", groupName, groupProperty.getSchemaValidationRules());
                           return Response.status(Status.OK).entity(ModelHelper.encode(groupProperty.getSchemaValidationRules())).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getSchemaValidationRules for group {} failed with exception: {}", groupName, exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void deleteGroup(String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Delete group called for group {}", groupName);
        registryService.deleteGroup(groupName)
                       .thenApply(status -> {
                           log.info("Group {} deleted", groupName);
                           return Response.status(Status.OK).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("deleteGroup failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getGroupSchemas(String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group schemas called for group {}", groupName);
        registryService.getGroupEvolutionHistory(groupName, null)
                       .thenApply(schemasEvolutionList -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemasEvolutionList.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           log.info("GetGroupSchemas: {} schemas found for group {}", list.getSchemas().size(), groupName);
                           return Response.status(Status.OK).entity(list).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }

                           log.warn("getGroupSchemas failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getLatestGroupSchema(String groupName, SecurityContext securityContext,
                                     AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get latest group schema called for group {}", groupName);
        registryService.getLatestSchema(groupName, null)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           log.info("Latest schema for group {} has version {}", groupName, schemaWithVersion.getVersion());
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }

                           log.warn("getLatestGroupSchema failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void addSchemaToGroupIfAbsent(String groupName, AddSchemaToGroupRequest addSchemaRequest,
                                         SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Add schema to group called for group {}", groupName);
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());

        registryService.addSchema(groupName, schemaInfo)
                       .thenApply(versionInfo -> {
                           VersionInfo version = ModelHelper.encode(versionInfo);
                           log.info("schema added to group {} with new version {}", groupName, versionInfo);
                           return Response.status(Status.CREATED).entity(version).build();
                       })
                       .exceptionally(exception -> {
                           Throwable unwrap = Exceptions.unwrap(exception);
                           if (unwrap instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           } else if (unwrap instanceof IncompatibleSchemaException) {
                               log.info("addSchemaToGroupIfAbsent incompatible schema {}", groupName);
                               return Response.status(Status.CONFLICT).build();
                           } else if (unwrap instanceof SchemaTypeMismatchException) {
                               log.info("addSchemaToGroupIfAbsent schema type mismatched {}", groupName);
                               return Response.status(Status.EXPECTATION_FAILED).build();
                           } else {
                               log.warn("addSchemaToGroupIfAbsent failed with exception: ", unwrap);
                               return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                           }
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void validate(String groupName, ValidateRequest validateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Validate schema called for group {}", groupName);

        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
        registryService.validateSchema(groupName, schemaInfo)
                       .thenApply(compatible -> {
                           log.info("Schema is valid for group {}", groupName);
                           return Response.status(Status.OK).entity(new Valid().valid(compatible)).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }                           
                           log.warn("validate failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void canRead(String groupName, CanReadRequest canReadRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Can read using schema called for group {}", groupName);

        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(canReadRequest.getSchemaInfo());
        registryService.canRead(groupName, schemaInfo)
                       .thenApply(canRead -> {
                           log.info("For group {}, can read using schema response = {}", groupName, canRead);
                           return Response.status(Status.OK).entity(new CanRead().compatible(canRead)).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("can read failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaFromVersion(String groupName, Integer version, 
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get schema from version {} called for group {}", version, groupName);
        registryService.getSchema(groupName, version)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                           log.info("Schema for version {} for group {} found.", version, groupName);
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} or version {} not found", groupName, version);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getSchemaFromVersion failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getOrGenerateEncodingId(String groupName, GetEncodingIdRequest getEncodingIdRequest,
                                        SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getOrGenerateEncodingId called for group {} with version {} and codec {}", groupName, 
                getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType());
        io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        CodecType codecType = ModelHelper.decode(getEncodingIdRequest.getCodecType());
        registryService.getEncodingId(groupName, version, codecType)
                       .thenApply(encodingId -> {
                           EncodingId id = ModelHelper.encode(encodingId);
                           log.info("For group {} with version {} and codec {}, returning encoding id {}", groupName,
                                   getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType(), id);
                           return Response.status(Status.OK).entity(id).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           } else if (Exceptions.unwrap(exception) instanceof CodecNotFoundException) {
                               log.info("getOrGenerateEncodingId failed Codec Not Found {}", groupName);
                               return Response.status(Status.PRECONDITION_FAILED).build();
                           } else {
                               log.warn("getOrGenerateEncodingId failed with exception: ", exception);
                               return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                           }
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaVersion(String groupName, Long fingerprint, GetSchemaVersion getSchemaVersion, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get schema version called for group {}", groupName);
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(getSchemaVersion.getSchemaInfo());

        registryService.getSchemaVersion(groupName, schemaInfo)
                       .thenApply(version -> {
                           VersionInfo versionInfo = ModelHelper.encode(version);
                           log.info("schema version {} found for group {}", versionInfo, groupName);
                           return Response.status(Status.OK).entity(versionInfo).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} or schema not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           
                           log.warn("getSchemaVersion failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getObjectTypeSchemas(String groupName, String objectTypeName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getObjectTypeSchemas called for group {} objectType {}", groupName, objectTypeName);
        registryService.getGroupEvolutionHistory(groupName, objectTypeName)
                       .thenApply(schemaEpochs -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemaEpochs.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           log.info("Found {} object type schemas for group {} and object type {}", list.getSchemas().size(), groupName, objectTypeName);
                           return Response.status(Status.OK).entity(list).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getObjectTypeSchemas failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getObjectTypes(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getObjectTypes called for group {} ", groupName);
        registryService.getObjectTypes(groupName, null)
                       .thenApply(objectTypes -> {
                           ObjectTypesList objectTypesList = new ObjectTypesList().objectTypes(objectTypes.getList());
                           log.info("Found object types {} for group {} ", objectTypesList, groupName);
                           return Response.status(Status.OK).entity(objectTypesList).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getObjectTypes failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });

    }

    @Override
    public void getLatestSchemaForObjectType(String groupName, String objectTypeName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getLatestSchemaForObjectType called for group {} object type {}", groupName, objectTypeName);
        registryService.getLatestSchema(groupName, objectTypeName)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           log.info("Latest schema for group {} object type {} has version {} ", groupName, objectTypeName, schema.getVersion());
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getLatestSchemaForObjectType failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }
    
    @Override
    public void getEncodingInfo(String groupName, Integer encodingId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getEncodingInfo called for group {} encodingId {}", groupName, encodingId);
        io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
        registryService.getEncodingInfo(groupName, id)
                       .thenApply(encodingInfo -> {
                           EncodingInfo encoding = ModelHelper.encode(encodingInfo);
                           log.info("group {} encoding id {} encodingInfo {}", groupName, encodingId, encoding);
                           return Response.status(Status.OK).entity(encoding).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getEncodingInfo failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }


    @Override
    public void getCodecsList(String groupName, SecurityContext securityContext,
                              AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getCodecsList called for group {} ", groupName);
        registryService.getCodecTypes(groupName)
                       .thenApply(list -> {
                           CodecsList codecsList = new CodecsList()
                                   .codecTypes(list.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           log.info("group {}, codecs {} ", groupName, codecsList);
                           return Response.status(Status.OK).entity(codecsList).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("getCodecsList failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void addCodec(String groupName, AddCodec addCodec, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("addCodec called for group {} codec {}", groupName, addCodec.getCodec());
        registryService.addCodec(groupName, ModelHelper.decode(addCodec.getCodec()))
                       .thenApply(v -> {
                           log.info("codec {} added to group {}", addCodec.getCodec(), groupName);
                           return Response.status(Status.CREATED).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                               log.warn("Group {} not found", groupName);
                               return Response.status(Status.NOT_FOUND).build();
                           }
                           log.warn("addCodec failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });

    }
}
