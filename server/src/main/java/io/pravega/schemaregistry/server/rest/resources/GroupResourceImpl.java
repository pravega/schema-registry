/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import com.google.common.base.Strings;
import io.pravega.auth.AuthException;
import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.common.FuturesCollector;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.CanRead;
import io.pravega.schemaregistry.contract.generated.rest.model.CodecTypesList;
import io.pravega.schemaregistry.contract.generated.rest.model.CreateGroupRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingId;
import io.pravega.schemaregistry.contract.generated.rest.model.EncodingInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.GetEncodingIdRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistory;
import io.pravega.schemaregistry.contract.generated.rest.model.ListGroupsResponse;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaVersionsList;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaWithVersion;
import io.pravega.schemaregistry.contract.generated.rest.model.UpdateValidationRulesRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.Valid;
import io.pravega.schemaregistry.contract.generated.rest.model.ValidateRequest;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static javax.ws.rs.core.Response.Status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class GroupResourceImpl extends AbstractResource implements ApiV1.GroupsApiAsync {
    private static final int DEFAULT_LIST_GROUPS_LIMIT = 100;
    
    public GroupResourceImpl(SchemaRegistryService registryService, ServiceConfig config, Executor executor) {
        super(registryService, config, executor);
    }

    @Override
    public void listGroups(String continuationToken, Integer limit, String namespace,
                           AsyncResponse asyncResponse) {
        log.info("List Groups called");
        int toFetch = limit == null ? DEFAULT_LIST_GROUPS_LIMIT : limit;
        ListGroupsResponse groupsList = new ListGroupsResponse();

        List<String> authorizationHeader = getConfig().isAuthEnabled() ? getAuthorizationHeader() : Collections.emptyList();
        CompletableFuture.runAsync(() -> {
            if (getConfig().isAuthEnabled()) {
                String credentials = parseCredentials(authorizationHeader);
                try {
                    getAuthManager().authenticate(credentials);
                } catch (AuthException e) {
                    log.warn("Unauthenticated", e);
                    asyncResponse.resume(Response.status(Response.Status.FORBIDDEN.getStatusCode()).build());
                }
            }
        }, getExecutorService()).thenCompose(v -> {
            Predicate<Map.Entry<String, GroupProperties>> predicate = x -> {
                try {
                    String resource = Strings.isNullOrEmpty(namespace) ?
                            String.format(AuthResources.GROUP_FORMAT, x.getKey()) :
                            String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, x.getKey());
                    authenticateAuthorize(authorizationHeader, resource, READ);
                    return true;
                } catch (AuthException e) {
                    return false;
                }
            };
            
            return FuturesCollector.filteredWithTokenAndLimit(
                    (ContinuationToken t, Integer l) ->
                            getRegistryService().listGroups(namespace, t, l)
                                           .thenApply(mwt -> new AbstractMap.SimpleEntry<>(mwt.getToken(),
                                                   new ArrayList<>(mwt.getMap().entrySet()))), 
                    predicate, ContinuationToken.fromString(continuationToken), toFetch, getExecutorService())
                                   .thenAccept(result -> {
                                          String contToken = result.getKey() == null ?
                                                  ContinuationToken.EMPTY.toString() : result.getKey().toString();
                                          groupsList.groups(
                                                  result.getValue().stream().collect(
                                                          Collectors.toMap(Map.Entry::getKey, x -> ModelHelper.encode(x.getValue()))))
                                                    .setContinuationToken(contToken);
                                      })
                                   .thenApply(r -> Response.status(Status.OK).entity(groupsList).build())
                                   .exceptionally(exception -> {
                       log.warn("listGroups failed with exception: ", exception);
                       return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                   })
                                   .thenApply(response -> {
                       asyncResponse.resume(response);
                       return response;
                   });
        });
    }

    @Override
    public void createGroup(CreateGroupRequest createGroupRequest, String namespace,
                            AsyncResponse asyncResponse) {
        String resource = Strings.isNullOrEmpty(namespace) ? AuthResources.ROOT : String.format(AuthResources.NAMESPACE_FORMAT, namespace);
        withCompletion("createGroup", READ_UPDATE, resource, asyncResponse, () -> {
            GroupProperties groupProperties = ModelHelper.decode(createGroupRequest.getGroupProperties());
            String groupName = createGroupRequest.getGroupName();
            return getRegistryService().createGroup(namespace, groupName, groupProperties)
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
    public void getGroupProperties(String groupName, String namespace,
                                   AsyncResponse asyncResponse) {
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) : 
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);
        withCompletion("getGroupProperties", READ, resource, asyncResponse,
                () -> getRegistryService().getGroupProperties(namespace, groupName)
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
    public void getGroupHistory(String groupName, String namespace, AsyncResponse asyncResponse) {
        log.info("Get group history called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);
        withCompletion("getGroupHistory", READ, resource, asyncResponse,
                () -> getRegistryService().getGroupHistory(namespace, groupName, null)
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
    public void updateSchemaValidationRules(String groupName, UpdateValidationRulesRequest updateValidationRulesRequest, 
                                            String namespace, AsyncResponse asyncResponse) {
        log.info("Update schema validation rules called for group {} with new request {}", groupName, updateValidationRulesRequest);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("updateSchemaValidationRules", READ_UPDATE, resource, asyncResponse,
                () -> {
                    SchemaValidationRules rules = ModelHelper.decode(updateValidationRulesRequest.getValidationRules());
                    SchemaValidationRules previousRules = updateValidationRulesRequest.getPreviousRules() == null ?
                            null : ModelHelper.decode(updateValidationRulesRequest.getPreviousRules());
                    return getRegistryService().updateSchemaValidationRules(namespace, groupName, rules, previousRules)
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
    public void deleteGroup(String groupName, String namespace,
                            AsyncResponse asyncResponse) {
        log.info("Delete group called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);
        withCompletion("deleteGroup", READ_UPDATE, resource, asyncResponse,
                () -> getRegistryService().deleteGroup(namespace, groupName)
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
    public void getSchemaVersions(String groupName, String type, String namespace, AsyncResponse asyncResponse) {
        log.info("Get group schemas called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getSchemaVersions", READ, resource, asyncResponse,
                () -> getRegistryService().getGroupHistory(namespace, groupName, null)
                                     .thenApply(history -> {
                                         SchemaVersionsList list = new SchemaVersionsList()
                                                 .schemas(history.stream().map(x -> new SchemaWithVersion()
                                                         .schemaInfo(ModelHelper.encode(x.getSchema()))
                                                         .version(ModelHelper.encode(x.getVersion())))
                                                                 .collect(Collectors.toList()));
                                         log.info("getSchemaVersions: {} schemas found for group {}", list.getSchemas().size(), groupName);
                                         return Response.status(Status.OK).entity(list).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }

                                         log.warn("getSchemaVersions failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addSchema(String groupName, SchemaInfo schemaInfo, String namespace,
                                          AsyncResponse asyncResponse) {
        log.info("Add schema to group called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_SCHEMA_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_SCHEMA_FORMAT, namespace, groupName);

        withCompletion("addSchema", READ_UPDATE, resource, asyncResponse,
                () -> {
                    return getRegistryService().addSchema(namespace, groupName, ModelHelper.decode(schemaInfo))
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
                                              } else if (unwrap instanceof SerializationFormatMismatchException) {
                                                  log.info("addSchema serialization format mismatched {}", groupName);
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
    public void validate(String groupName, ValidateRequest validateRequest, String namespace, AsyncResponse asyncResponse) {
        log.info("Validate schema called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("validate", READ, resource, asyncResponse,
                () -> {
                    return getRegistryService().validateSchema(namespace, groupName, ModelHelper.decode(validateRequest.getSchemaInfo()))
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
    public void canRead(String groupName, SchemaInfo schemaInfo, String namespace, AsyncResponse asyncResponse) {
        log.info("Can read using schema called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("canRead", READ, resource, asyncResponse,
                () -> {
                    return getRegistryService().canRead(namespace, groupName, ModelHelper.decode(schemaInfo))
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
    public void getSchemaForId(String groupName, Integer schemaId, String namespace, AsyncResponse asyncResponse) {
        log.info("Get schema from version {} called for group {}", schemaId, groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getSchemaForId", READ, resource, asyncResponse,
                () -> getRegistryService().getSchema(namespace, groupName, schemaId)
                                     .thenApply(schemaWithVersion -> {
                                         SchemaInfo schema = ModelHelper.encode(schemaWithVersion);
                                         log.info("Schema for version {} for group {} found.", schemaId, groupName);
                                         return Response.status(Status.OK).entity(schema).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {} not found", groupName, schemaId);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("getSchemaForId failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getSchemaFromVersion(String groupName, String schemaType, Integer version, String namespace, AsyncResponse asyncResponse) {
        log.info("Get schema from version {} called for group {}", version, groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getSchemaFromVersion", READ, resource, asyncResponse,
                () -> getRegistryService().getSchema(namespace, groupName, schemaType, version)
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
    public void deleteSchemaForId(String groupName, Integer schemaId, String namespace,
                                               AsyncResponse asyncResponse) {
        log.info("Delete schema from version {} called for group {}", schemaId, groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("deleteSchemaForId", READ_UPDATE, resource, asyncResponse,
                () -> getRegistryService().deleteSchema(namespace, groupName, schemaId)
                                     .thenApply(v -> {
                                         log.info("Schema for version {} for group {} deleted.", schemaId, groupName);
                                         return Response.status(Status.NO_CONTENT).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {} not found", groupName, schemaId);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("deleteSchemaForId failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void deleteSchemaVersion(String groupName, String schemaType, Integer version, String namespace,
                                    AsyncResponse asyncResponse) {
        log.info("Delete schema from version {}/{} called for group {}", schemaType, version, groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_SCHEMA_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_SCHEMA_FORMAT, namespace, groupName);

        withCompletion("deleteSchemaVersion", READ_UPDATE, resource, asyncResponse,
                () -> getRegistryService().deleteSchema(namespace, groupName, schemaType, version)
                                     .thenApply(v -> {
                                         log.info("Schema for version {}/{} for group {} deleted.", schemaType, version, groupName);
                                         return Response.status(Status.NO_CONTENT).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} or version {}/{} not found", groupName, schemaType, version);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("deleteSchemaVersion failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void getEncodingId(String groupName, GetEncodingIdRequest getEncodingIdRequest, String namespace,
                              AsyncResponse asyncResponse) {
        log.info("getEncodingId called for group {} with version {} and codec {}", groupName,
                getEncodingIdRequest.getVersionInfo(), getEncodingIdRequest.getCodecType());
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getEncodingId", READ, resource, asyncResponse,
                () -> {
                    io.pravega.schemaregistry.contract.data.VersionInfo version = ModelHelper.decode(getEncodingIdRequest.getVersionInfo());
                    String codecType = getEncodingIdRequest.getCodecType();
                    return getRegistryService().getEncodingId(namespace, groupName, version, codecType)
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
                                              } else if (Exceptions.unwrap(exception) instanceof CodecTypeNotRegisteredException) {
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
    public void getSchemaVersion(String groupName, SchemaInfo schemaInfo, String namespace, AsyncResponse asyncResponse) {
        log.info("Get schema version called for group {}", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getSchemaVersion", READ, resource, asyncResponse,
                () -> {
                    return getRegistryService().getSchemaVersion(namespace, groupName, ModelHelper.decode(schemaInfo))
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
    public void getSchemas(String groupName, String type, String namespace, AsyncResponse asyncResponse) {
        log.info("getSchemas called for group {} ", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getSchemas", READ, resource, asyncResponse,
                () -> getRegistryService().getSchemas(namespace, groupName, type)
                                                          .thenApply(schemas -> {
                                                              SchemaVersionsList schemaList = new SchemaVersionsList()
                                                                      .schemas(schemas.stream().map(ModelHelper::encode).collect(Collectors.toList()));
                                                              List<String> names = schemaList.getSchemas().stream().map(x -> x.getSchemaInfo().getType()).collect(Collectors.toList());
                                                              log.info("Found schemas {} for group {} ", names, groupName);
                                                              return Response.status(Status.OK).entity(schemaList).build();
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
    public void getEncodingInfo(String groupName, Integer encodingId, String namespace, AsyncResponse asyncResponse) {
        log.info("getEncodingInfo called for group {} encodingId {}", groupName, encodingId);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getEncodingInfo", READ, resource, asyncResponse,
                () -> {
                    io.pravega.schemaregistry.contract.data.EncodingId id = new io.pravega.schemaregistry.contract.data.EncodingId(encodingId);
                    return getRegistryService().getEncodingInfo(namespace, groupName, id)
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
    public void getCodecTypesList(String groupName, String namespace,
                                  AsyncResponse asyncResponse) {
        log.info("getCodecTypesList called for group {} ", groupName);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_FORMAT, namespace, groupName);

        withCompletion("getCodecTypesList", READ, resource, asyncResponse,
                () -> getRegistryService().getCodecTypes(namespace, groupName)
                                     .thenApply(list -> {
                                         CodecTypesList codecsList = new CodecTypesList().codecTypes(list);
                                         log.info("group {}, codecTypes {} ", groupName, codecsList);
                                         return Response.status(Status.OK).entity(codecsList).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("getCodecTypesList failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }

    @Override
    public void addCodecType(String groupName, String codecType, String namespace, AsyncResponse asyncResponse) {
        log.info("addCodecType called for group {} codecType {}", groupName, codecType);
        String resource = Strings.isNullOrEmpty(namespace) ? String.format(AuthResources.GROUP_CODEC_FORMAT, groupName) :
                String.format(AuthResources.NAMESPACE_GROUP_CODEC_FORMAT, namespace, groupName);

        withCompletion("addCodecType", READ, resource, asyncResponse,
                () -> getRegistryService().addCodecType(namespace, groupName, codecType)
                                     .thenApply(v -> {
                                         log.info("codecType {} added to group {}", codecType, groupName);
                                         return Response.status(Status.CREATED).build();
                                     })
                                     .exceptionally(exception -> {
                                         if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                             log.warn("Group {} not found", groupName);
                                             return Response.status(Status.NOT_FOUND).build();
                                         }
                                         log.warn("addCodecType failed with exception: ", exception);
                                         return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                     }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }
}
