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
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaForObjectTypeByVersionRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaFromVersionRequest;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

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
        registryService.listGroups(null)
                       .thenApply(groups -> {
                           GroupsList groupsList = new GroupsList();
                           groups.getMap().forEach((x, y) -> groupsList.addGroupsItem(ModelHelper.encode(x, y)));
                           return Response.status(Status.OK).entity(groupsList).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof NotFoundException) {
                               return Response.status(Status.NOT_FOUND).build();
                           }
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
        SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
        SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
        GroupProperties properties = new GroupProperties(
                schemaType, validationRules, createGroupRequest.isValidateByObjectType(), createGroupRequest.getProperties());
        String groupName = URLDecoder.decode(createGroupRequest.getGroupName(), StandardCharsets.UTF_8.toString());
        registryService.createGroup(groupName, properties)
                       .thenApply(createStatus -> {
                           if (!createStatus) {
                               return Response.status(Status.CONFLICT).build();
                           }
                           return Response.status(Status.OK).build();
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
        registryService.getGroupProperties(groupName)
                       .thenApply(groupProperty -> Response.status(Status.OK).entity(ModelHelper.encode(groupProperty)).build())
                       .exceptionally(exception -> {
                           log.warn("getGroupProperties failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
        SchemaValidationRules previousRules = updateValidationRulesPolicyRequest.getPreviousRules() == null ?
                null : ModelHelper.decode(updateValidationRulesPolicyRequest.getPreviousRules());
        registryService.updateSchemaValidationRules(groupName, rules, previousRules)
                       .thenApply(groupProperty -> Response.status(Status.OK).build())
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof PreconditionFailedException) {
                               log.info("updateSchemaValidationRules write conflict {}", groupName);
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
        throw new NotImplementedException("get schema validation rules");
    }

    @Override
    public void deleteGroup(String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        registryService.deleteGroup(groupName)
                       .thenApply(status -> {
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
        registryService.getGroupEvolutionHistory(groupName, null)
                       .thenApply(schemasEvolutionList -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemasEvolutionList.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(list).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.getLatestSchema(groupName, null)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
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
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());

        registryService.addSchemaIfAbsent(groupName, schemaInfo)
                       .thenApply(versionInfo -> {
                           VersionInfo version = ModelHelper.encode(versionInfo);
                           return Response.status(Status.OK).entity(version).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof IncompatibleSchemaException) {
                               log.info("addSchemaToGroupIfAbsent incompatible schema {}", groupName);
                               return Response.status(Status.CONFLICT).build();
                           } else if (Exceptions.unwrap(exception) instanceof SchemaTypeMismatchException) {
                               log.info("addSchemaToGroupIfAbsent schema type mismatched {}", groupName);
                               return Response.status(Status.EXPECTATION_FAILED).build();
                           } else {
                               log.warn("addSchemaToGroupIfAbsent failed with exception: ", exception);
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
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
        registryService.validateSchema(groupName, schemaInfo)
                       .thenApply(compatible -> {
                           return Response.status(Status.OK).entity(new Valid().valid(compatible)).build();
                       })
                       .exceptionally(exception -> {
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
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(canReadRequest.getSchemaInfo());
        registryService.canRead(groupName, schemaInfo)
                       .thenApply(canRead -> {
                           return Response.status(Status.OK).entity(new CanRead().compatible(canRead)).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("can read failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
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
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
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
        io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
        CodecType codecType = ModelHelper.decode(getEncodingIdRequest.getCodecType());
        registryService.getEncodingId(groupName, version, codecType)
                       .thenApply(encodingId -> {
                           EncodingId id = ModelHelper.encode(encodingId);
                           return Response.status(Status.OK).entity(id).build();
                       })
                       .exceptionally(exception -> {
                           if (Exceptions.unwrap(exception) instanceof CodecNotFoundException) {
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
        io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(getSchemaVersion.getSchemaInfo());

        registryService.getSchemaVersion(groupName, schemaInfo)
                       .thenApply(version -> {
                           VersionInfo versionInfo = ModelHelper.encode(version);
                           return Response.status(Status.OK).entity(versionInfo).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.getGroupEvolutionHistory(groupName, objectTypeName)
                       .thenApply(schemaEpochs -> {
                           SchemaList list = new SchemaList()
                                   .schemas(schemaEpochs.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(list).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.getObjectTypes(groupName, null)
                       .thenApply(objectTypes -> {
                           ObjectTypesList objectTypesList = new ObjectTypesList().groups(objectTypes.getList());
                           return Response.status(Status.OK).entity(objectTypesList).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.getLatestSchema(groupName, objectTypeName)
                       .thenApply(schemaWithVersion -> {
                           SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("getLatestSchemaForObjectType failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });
    }

    @Override
    public void getSchemaFromVersionForObjectType(String groupName, String objectType, Integer versionId,
                                                  GetSchemaForObjectTypeByVersionRequest getSchemaForObjectTypeByVersionRequest,
                                                  SecurityContext securityContext, AsyncResponse asyncResponse)
            throws NotFoundException {
        io.pravega.schemaregistry.contract.data.VersionInfo versionInfo = ModelHelper.decode(
                getSchemaForObjectTypeByVersionRequest.getVersionInfo());
        registryService.getSchema(groupName, versionInfo)
                       .thenApply(schemaWithVersion -> {
                           SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                           return Response.status(Status.OK).entity(schema).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("getSchemaFromVersionForObjectType failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
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
                           return Response.status(Status.OK).entity(encoding).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.getCodecTypes(groupName)
                       .thenApply(list -> {
                           CodecsList codecsList = new CodecsList()
                                   .codecTypes(list.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                           return Response.status(Status.OK).entity(codecsList).build();
                       })
                       .exceptionally(exception -> {
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
        registryService.addCodec(groupName, ModelHelper.decode(addCodec.getCodec()))
                       .thenApply(v -> {
                           return Response.status(Status.OK).build();
                       })
                       .exceptionally(exception -> {
                           log.warn("addCodec failed with exception: ", exception);
                           return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                       })
                       .thenApply(response -> {
                           asyncResponse.resume(response);
                           return response;
                       });

    }
}
