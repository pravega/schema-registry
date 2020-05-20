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
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.contract.generated.rest.model.AddCodec;
import io.pravega.schemaregistry.contract.generated.rest.model.AddSchemaRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CanReadRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecsList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GetSchemaVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaNamesList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesPolicyRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.generated.rest.server.api.NotFoundException;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaRegistryResourceImpl implements ApiV1.GroupsApiAsync {
    private static final int DEFAULT_LIST_GROUPS_LIMIT = 100;
    
    @Context
    HttpHeaders headers;

    private SchemaRegistryService registryService;

    public SchemaRegistryResourceImpl(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    @Override
    public void listGroups(String continuationToken, Integer limit, SecurityContext securityContext,
                           AsyncResponse asyncResponse) throws NotFoundException {
        log.info("List Groups called");
        int limitUnboxed = limit == null ? DEFAULT_LIST_GROUPS_LIMIT : limit;
        withCompletion("listGroups", () -> registryService.listGroups(ContinuationToken.fromString(continuationToken), limitUnboxed)
                                                          .thenApply(result -> {
                                                              ListGroupsResponse groupsList = new ListGroupsResponse();
                                                              result.getMap().forEach((x, y) -> {
                                                                  if (y == null) {
                                                                      // partially created group.
                                                                      groupsList.putGroupsItem(x, null);
                                                                  } else {
                                                                      groupsList.putGroupsItem(x, ModelHelper.encode(y));
                                                                  }
                                                              });
                                                              groupsList.continuationToken(result.getToken() == null ? null : result.getToken().toString());
                                                              return Response.status(Status.OK).entity(groupsList).build();
                                                          })
                                                          .exceptionally(exception -> {
                                                              log.warn("listGroups failed with exception: ", exception);
                                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                          }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    private CompletableFuture<Response> withCompletion(String request, Supplier<CompletableFuture<Response>> future) {
        try {
            return future.get();
        } catch (IllegalArgumentException e) {
            log.warn("Bad request {}", request);
            return CompletableFuture.completedFuture(Response.status(Status.BAD_REQUEST).build());
        } catch (Exception e) {
            log.error("request failed with exception {}", e);
            return Futures.failedFuture(e);
        }
    }

    @Override
    public void createGroup(CreateGroupRequest createGroupRequest, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Create Group called with params {}", createGroupRequest);
        withCompletion("createGroup", () -> {
            SchemaType schemaType = ModelHelper.decode(createGroupRequest.getSchemaType());
            SchemaValidationRules validationRules = ModelHelper.decode(createGroupRequest.getValidationRules());
            GroupProperties properties = new GroupProperties(
                    schemaType, validationRules, createGroupRequest.isVersionedBySchemaName(), createGroupRequest.getProperties());
            String groupName = createGroupRequest.getGroupName();
            return registryService.createGroup(groupName, properties)
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
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getGroupProperties(String groupName, SecurityContext securityContext,
                                   AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group properties called for group {}", groupName);
        withCompletion("getGroupProperties", () -> registryService.getGroupProperties(groupName)
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
                                                                  }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getGroupHistory(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group history called for group {}", groupName);
        withCompletion("getGroupHistory", () -> registryService.getGroupHistory(groupName, null)
                                                               .thenApply(history -> {
                                                                   GroupHistory list = new GroupHistory()
                                                                           .history(history.stream().map(ModelHelper::encode)
                                                                                           .collect(Collectors.toList()));
                                                                   log.info("getGroupHistory: {} schemas found for group {}", list.getHistory().size(), groupName);
                                                                   return Response.status(Status.OK).entity(list).build();
                                                               })
                                                               .exceptionally(exception -> {
                                                                   if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                       log.warn("Group {} not found", groupName);
                                                                       return Response.status(Status.NOT_FOUND).build();
                                                                   }

                                                                   log.warn("getGroupHistory failed with exception: ", exception);
                                                                   return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                               }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });

    }

    @Override
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesPolicyRequest updateValidationRulesPolicyRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Update schema validation rules called for group {} with new request {}", groupName, updateValidationRulesPolicyRequest);
        withCompletion("updateSchemaValidationRules", () -> {
            SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesPolicyRequest.getValidationRules());
            SchemaValidationRules previousRules = updateValidationRulesPolicyRequest.getPreviousRules() == null ?
                    null : ModelHelper.decode(updateValidationRulesPolicyRequest.getPreviousRules());
            return registryService.updateSchemaValidationRules(groupName, rules, previousRules)
                                  .thenApply(groupProperty -> Response.status(Status.OK).build())
                                  .exceptionally(exception -> {
                                      if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                          log.warn("Group {} not found", groupName);
                                          return Response.status(Status.NOT_FOUND).build();
                                      } else if (Exceptions.unwrap(exception) instanceof PreconditionFailedException) {
                                          log.warn("updateSchemaValidationRules write conflict {}", groupName);
                                          return Response.status(Status.CONFLICT).build();
                                      } else {
                                          log.warn("updateSchemaValidationRules failed with exception: ", exception);
                                          return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                      }
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemaValidationRules(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group schema validation rules called for group {}", groupName);
        withCompletion("getSchemaValidationRules", () -> registryService.getGroupProperties(groupName)
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
                                                                        }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void deleteGroup(String groupName, SecurityContext securityContext,
                            AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Delete group called for group {}", groupName);
        withCompletion("deleteGroup", () -> registryService.deleteGroup(groupName)
                                                           .thenApply(status -> {
                                                               log.info("Group {} deleted", groupName);
                                                               return Response.status(Status.NO_CONTENT).build();
                                                           })
                                                           .exceptionally(exception -> {
                                                               log.warn("deleteGroup failed with exception: ", exception);
                                                               return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                           }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemas(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get group schemas called for group {}", groupName);
        withCompletion("getSchemas", () -> registryService.getGroupHistory(groupName, null)
                                                               .thenApply(history -> {
                                                                   SchemaVersionsList list = new SchemaVersionsList()
                                                                           .schemas(history.stream().map(x -> new SchemaWithVersion()
                                                                                   .schemaInfo(ModelHelper.encode(x.getSchema()))
                                                                                   .version(ModelHelper.encode(x.getVersion())))
                                                                                           .collect(Collectors.toList()));
                                                                   log.info("GetGroupSchemas: {} schemas found for group {}", list.getSchemas().size(), groupName);
                                                                   return Response.status(Status.OK).entity(list).build();
                                                               })
                                                               .exceptionally(exception -> {
                                                                   if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                       log.warn("Group {} not found", groupName);
                                                                       return Response.status(Status.NOT_FOUND).build();
                                                                   }

                                                                   log.warn("getSchemas failed with exception: ", exception);
                                                                   return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                               }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getLatestSchema(String groupName, SecurityContext securityContext,
                                AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get latest group schema called for group {}", groupName);
        withCompletion("getLatestSchema", () -> registryService.getGroupLatestSchemaVersion(groupName, null)
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

                                                                        log.warn("getLatestSchema failed with exception: ", exception);
                                                                        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                                    }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addSchema(String groupName, AddSchemaRequest addSchemaRequest,
                                         SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Add schema to group called for group {}", groupName);
        withCompletion("addSchema", () -> {
            io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(addSchemaRequest.getSchemaInfo());

            return registryService.addSchema(groupName, schemaInfo)
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
                                          log.info("addSchema incompatible schema {}", groupName);
                                          return Response.status(Status.CONFLICT).build();
                                      } else if (unwrap instanceof SchemaTypeMismatchException) {
                                          log.info("addSchema schema type mismatched {}", groupName);
                                          return Response.status(Status.EXPECTATION_FAILED).build();
                                      } else {
                                          log.warn("addSchema failed with exception: ", unwrap);
                                          return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                      }
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void validate(String groupName, ValidateRequest validateRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Validate schema called for group {}", groupName);

        withCompletion("validate", () -> {
            io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(validateRequest.getSchemaInfo());
            return registryService.validateSchema(groupName, schemaInfo)
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
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void canRead(String groupName, CanReadRequest canReadRequest, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Can read using schema called for group {}", groupName);

        withCompletion("canRead", () -> {
            io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(canReadRequest.getSchemaInfo());
            return registryService.canRead(groupName, schemaInfo)
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
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemaFromVersion(String groupName, Integer version,
                                     SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get schema from version {} called for group {}", version, groupName);
        withCompletion("getSchemaFromVersion", () -> registryService.getSchema(groupName, version)
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
                                                                    }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getEncodingId(String groupName, GetEncodingIdRequest getEncodingIdRequest,
                              SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getEncodingId called for group {} with version {} and codec {}", groupName,
                getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType());
        withCompletion("getEncodingId", () -> {
            io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
            CodecType codecType = ModelHelper.decode(getEncodingIdRequest.getCodecType());
            return registryService.getEncodingId(groupName, version, codecType)
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
                                          log.info("getEncodingId failed Codec Not Found {}", groupName);
                                          return Response.status(Status.PRECONDITION_FAILED).build();
                                      } else {
                                          log.warn("getEncodingId failed with exception: ", exception);
                                          return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                      }
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemaVersion(String groupName, GetSchemaVersion getSchemaVersion, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("Get schema version called for group {}", groupName);
        withCompletion("getSchemaVersion", () -> {
            io.pravega.schemaregistry.contract.data.SchemaInfo schemaInfo = ModelHelper.decode(getSchemaVersion.getSchemaInfo());

            return registryService.getSchemaVersion(groupName, schemaInfo)
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
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }

    @Override
    public void getSchemasForSchemaName(String groupName, String schemaName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getSchemaNameSchemas called for group {} schemaName {}", groupName, schemaName);
        withCompletion("getObjectSchemas", () -> registryService.getGroupHistory(groupName, schemaName)
                                                                .thenApply(history -> {
                                                                    SchemaVersionsList list = new SchemaVersionsList()
                                                                            .schemas(history.stream().map(x -> new SchemaWithVersion()
                                                                                    .schemaInfo(ModelHelper.encode(x.getSchema()))
                                                                                    .version(ModelHelper.encode(x.getVersion())))
                                                                                            .collect(Collectors.toList()));
                                                                    log.info("Found {} object type schemas for group {} and object type {}", list.getSchemas().size(), groupName, schemaName);
                                                                    return Response.status(Status.OK).entity(list).build();
                                                                })
                                                                .exceptionally(exception -> {
                                                                    if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                        log.warn("Group {} not found", groupName);
                                                                        return Response.status(Status.NOT_FOUND).build();
                                                                    }
                                                                    log.warn("getSchemaNameSchemas failed with exception: ", exception);
                                                                    return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                                }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemaNames(String groupName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getSchemaNames called for group {} ", groupName);
        withCompletion("getObjects", () -> registryService.getSchemaNames(groupName)
                                                          .thenApply(schemaNames -> {
                                                              SchemaNamesList schemaNamesList = new SchemaNamesList().objects(schemaNames);
                                                              log.info("Found object types {} for group {} ", schemaNamesList, groupName);
                                                              return Response.status(Status.OK).entity(schemaNamesList).build();
                                                          })
                                                          .exceptionally(exception -> {
                                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                  log.warn("Group {} not found", groupName);
                                                                  return Response.status(Status.NOT_FOUND).build();
                                                              }
                                                              log.warn("getSchemaNames failed with exception: ", exception);
                                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                          }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });

    }

    @Override
    public void getLatestSchemaForSchemaName(String groupName, String schemaNameName, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getLatestSchemaForSchemaName called for group {} object type {}", groupName, schemaNameName);
        withCompletion("getLatestSchemaForSchemaName", () -> registryService.getGroupLatestSchemaVersion(groupName, schemaNameName)
                                                                            .thenApply(schemaWithVersion -> {
                                                                                SchemaWithVersion schema = ModelHelper.encode(schemaWithVersion);
                                                                                log.info("Latest schema for group {} object type {} has version {} ", groupName, schemaNameName, schema.getVersion());
                                                                                return Response.status(Status.OK).entity(schema).build();
                                                                            })
                                                                            .exceptionally(exception -> {
                                                                                if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                                                    log.warn("Group {} not found", groupName);
                                                                                    return Response.status(Status.NOT_FOUND).build();
                                                                                }
                                                                                log.warn("getLatestSchemaForSchemaName failed with exception: ", exception);
                                                                                return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                                                            }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getEncodingInfo(String groupName, Integer encodingId, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getEncodingInfo called for group {} encodingId {}", groupName, encodingId);
        withCompletion("getEncodingInfo", () -> {
            io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
            return registryService.getEncodingInfo(groupName, id)
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
                                  });
        }).thenApply(response -> {
            asyncResponse.resume(response);
            return response;
        });
    }


    @Override
    public void getCodecsList(String groupName, SecurityContext securityContext,
                              AsyncResponse asyncResponse) throws NotFoundException {
        log.info("getCodecsList called for group {} ", groupName);
        withCompletion("getCodecsList", () -> registryService.getCodecTypes(groupName)
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
                                                             }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addCodec(String groupName, AddCodec addCodec, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        log.info("addCodec called for group {} codec {}", groupName, addCodec.getCodec());
        withCompletion("addCodec", () -> registryService.addCodec(groupName, ModelHelper.decode(addCodec.getCodec()))
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
                                                        }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });

    }
}
