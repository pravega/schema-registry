/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ResultPage;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.common.FuturesUtility;
import io.pravega.schemaregistry.common.NameUtil;
import io.pravega.schemaregistry.contract.data.BackwardAndForward;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.exceptions.SerializationFormatMismatchException;
import io.pravega.schemaregistry.rules.CompatibilityChecker;
import io.pravega.schemaregistry.rules.CompatibilityCheckerFactory;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTransitive;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTransitive;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardPolicy;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardPolicy;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.ForwardTill;
import static io.pravega.schemaregistry.contract.data.BackwardAndForward.BackwardTill;

/**
 * Schema registry service backend.
 */
@Slf4j
public class SchemaRegistryService {
    private static final Retry.RetryAndThrowConditionally RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                       .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DRAFT_04_SCHEMA = "http://json-schema.org/draft-04/schema#";
    private static final String DRAFT_06_SCHEMA = "http://json-schema.org/draft-06/schema#";
    private static final String DRAFT_07_SCHEMA = "http://json-schema.org/draft-07/schema#";
    private static final String SCHEMA_NODE = "$schema";

    private static final VersionInfo EMPTY_VERSION = new VersionInfo("", -1, -1);

    static {
        OBJECT_MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        OBJECT_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }
    
    private final SchemaStore store;

    private final ScheduledExecutorService executor;

    public SchemaRegistryService(SchemaStore store, ScheduledExecutorService executor) {
        this.store = store;
        this.executor = executor;
    }

    /**
     * Lists groups with pagination.
     *
     * @param namespace         namespace for which the request is scoped to.
     * @param continuationToken continuation token.
     * @param limit             max number of groups to return.
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    public CompletableFuture<ResultPage<Map.Entry<String, GroupProperties>, ContinuationToken>> listGroups(String namespace, ContinuationToken continuationToken, int limit) {
        log.debug("List groups called");
        return FuturesUtility.filteredWithTokenAndLimit(
                (ContinuationToken c, Integer l) ->
                        store.listGroups(namespace, c, l)
                             .thenCompose(reply -> {
                                 List<String> list = reply.getList();
                                 return Futures.allOfWithResults(list.stream().map(x -> 
                                         Futures.exceptionallyExpecting(store.getGroupProperties(namespace, x),
                                         e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                                         null)
                                                .thenApply(prop -> new AbstractMap.SimpleEntry<>(x, prop)))
                                                                     .collect(Collectors.toList()))
                                               .thenApply(result -> new AbstractMap.SimpleEntry<>(reply.getToken(), result));
                             }),
                x -> x.getValue() != null, continuationToken, limit, executor)
                             .thenApply(groupsList -> {
                                   log.debug("Returning groups {}", namespace, groupsList);
                                   List<Map.Entry<String, GroupProperties>> collect = groupsList
                                           .getValue().stream().filter(x -> x.getValue() != null)
                                           .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue())).collect(Collectors.toList());
                                   return new ResultPage<>(collect, groupsList.getKey());
                               });
    }

    /**
     * Creates new group. Idempotent behaviour. If group already exists, it returns false.
     *
     * @param namespace       namespace for which the request is scoped to.
     * @param group           Name of group.
     * @param groupProperties Group properties.
     * @return CompletableFuture which is completed when create group completes. True indicates this was
     * new group, false indicates it was an existing group.
     */
    public CompletableFuture<Boolean> createGroup(String namespace, String group, GroupProperties groupProperties) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(groupProperties != null);
        Preconditions.checkArgument(isValidCompatibilityForFormat(groupProperties.getSerializationFormat(), groupProperties.getCompatibility()));
        log.debug("create group called for {} {} with group properties {}", namespace, group, groupProperties);
        return store.createGroup(namespace, group, groupProperties)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            if (r) {
                                log.debug("Group {} {} created successfully.", namespace, group);
                            } else {
                                log.debug("Group {} {} already exists.", namespace, group);
                            }
                        } else {
                            log.warn("create group {} {} request failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Gets group's properties.
     * {@link GroupProperties#serializationFormat} which identifies the serialization format used to describe the schema.
     * {@link GroupProperties#compatibility} sets the schema compatibility policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#allowMultipleTypes} that specifies multiple schemas with distinct {@link SchemaInfo#type} can
     * be registered.
     * {@link GroupProperties#properties} properties for a group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @return CompletableFuture which holds group properties upon completion.
     */
    public CompletableFuture<GroupProperties> getGroupProperties(String namespace, String group) {
        Preconditions.checkArgument(group != null);
        log.debug("getGroupProperties called for group {} {}.", namespace, group);
        return store.getGroupProperties(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {} properties.", namespace, group);
                        } else {
                            log.warn("getGroupProperties for group {} {} request failed with error", namespace, group, e);
                        }
                    });

    }

    /**
     * Update group's schema compatibility policy. If previous compatibility are sent, a conditional update is performed.
     *
     * @param namespace       namespace for which the request is scoped to.
     * @param group           Name of group.
     * @param compatibility New compatibility for the group.
     * @param previousCompatibility   Previous compatibility for the group. If null, unconditional update is performed.
     * @return CompletableFuture which is completed when compatibility policy update completes.
     */
    public CompletableFuture<Void> updateCompatibility(String namespace, String group, Compatibility compatibility,
                                                       @Nullable Compatibility previousCompatibility) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(compatibility != null);
        log.debug("updateCompatibility called for group {} {}. New compatibility {}", namespace, group, compatibility);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(pos -> {
                                             return store.getGroupProperties(namespace, group)
                                                         .thenCompose(prop -> {
                                                             if (previousCompatibility == null) {
                                                                 return store.updateCompatibility(namespace, group, pos, compatibility);
                                                             } else {
                                                                 if (previousCompatibility.equals(prop.getCompatibility())) {
                                                                     return store.updateCompatibility(namespace, group, pos, compatibility);
                                                                 } else {
                                                                     throw new PreconditionFailedException("Conditional update failed");
                                                                 }
                                                             }
                                                         });
                                         })
                                         .whenComplete((r, e) -> {
                                             if (e == null) {
                                                 log.debug("Group {} {} updateCompatibility successful.", namespace, group);
                                             } else {
                                                 log.warn("getGroupProperties for group {} {} request failed with error", namespace, group, e);
                                             }
                                         }), executor);
    }

    /**
     * Gets list of latest schema versions for all schemas registered in the group. Schemas representing different
     * object types are identified by {@link SchemaInfo#type}.
     *
     * @param namespace  namespace for which the request is scoped to.
     * @param group      Name of group.
     * @param schemaType type of object as identified by {@link SchemaInfo#type}.
     * @return CompletableFuture which holds list of latest schema versions for different schemas upon completion.
     */
    public CompletableFuture<List<SchemaWithVersion>> getSchemas(String namespace, String group, @Nullable String schemaType) {
        Preconditions.checkArgument(group != null);
        log.debug("getSchemas called for group {} {}. New compatibility {}", namespace, group);

        if (schemaType == null) {
            return store.listLatestSchemas(namespace, group)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.debug("Group {} {} getSchemas {}.", namespace, group, r);
                            } else {
                                log.warn("getSchemas for group {} {} request failed with error", namespace, group, e);
                            }
                        });
        } else {
            return store.getLatestSchemaVersion(namespace, group, schemaType)
                        .thenApply(Collections::singletonList);
        }
    }

    /**
     * Adds schema to the group. If group is configured with {@link GroupProperties#allowMultipleTypes}, then
     * the {@link SchemaInfo#type} is used to filter previous schemas and apply schema compatibility policy against all
     * previous versions of schema.
     * Compatibility that are sent to the registry should be a super set of Compatibility set in
     * {@link GroupProperties#compatibility}
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @param schemaInfo    Schema to add.
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.
     */
    public CompletableFuture<VersionInfo> addSchema(String namespace, String group, SchemaInfo schemaInfo) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schemaInfo != null);
        log.debug("addSchema called for group {} {}. schema {}", namespace, group, schemaInfo.getType());
        SchemaInfo schema = normalizeSchemaBinary(schemaInfo);
        // 1. get group policy
        // 2. get checker for serialization format.
        // validate schema against group compatibility policy on schema
        // 3. conditionally update the schema
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.getGroupProperties(namespace, group)
                                                      .thenCompose(prop -> {
                                                          return Futures.exceptionallyComposeExpecting(store.getSchemaVersion(namespace, group, schema),
                                                                  e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                                                                  () -> { // Schema doesnt exist. Validate and add it
                                                                      return validateSchema(namespace, group, schema, prop.getCompatibility())
                                                                              .thenCompose(valid -> {
                                                                                  if (!valid) {
                                                                                      throw new IncompatibleSchemaException(String.format("%s is incompatible", schema.getType()));
                                                                                  }
                                                                                  return store.addSchema(namespace, group, schema, prop, etag);
                                                                              });
                                                                  });
                                                      })), executor)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, schema {} added successfully.", namespace, group, schema.getType());
                        } else {
                            log.warn("Group {} {}, schema {} add failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Gets schema corresponding to the version.
     *
     * @param namespace      namespace for which the request is scoped to.
     * @param group          Name of group.
     * @param schemaId Version which uniquely identifies schema within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String group, int schemaId) {
        log.debug("Group {} {}, get schema for version {} .", namespace, group, schemaId);

        return store.getSchema(namespace, group, schemaId)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, return schema for verison {}.", namespace, group, schemaId);
                        } else {
                            log.warn("Group {} {}, get schema version {} failed with error", namespace, group, schemaId, e);
                        }
                    });
    }

    /**
     * Gets schema corresponding to the version.
     *
     * @param namespace  namespace for which the request is scoped to.
     * @param group      Name of group.
     * @param schemaType Schema type as used in {@link SchemaInfo#type}
     * @param version    Version number which uniquely identifies schema of schemaType within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<SchemaInfo> getSchema(String namespace, String group, String schemaType, int version) {
        log.debug("Group {} {}, get schema for version {}/{}.", namespace, group, schemaType, version);

        return store.getSchema(namespace, group, schemaType, version)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, return schema for verison {}/{}.", namespace, group, schemaType, version);
                        } else {
                            log.warn("Group {} {}, get schema version {}/{} failed with error", namespace, group, schemaType, version, e);
                        }
                    });
    }

    /**
     * Delete schema corresponding to the version.
     *
     * @param namespace      namespace for which the request is scoped to.
     * @param group          Name of group.
     * @param schemaId Version which uniquely identifies schema within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<Void> deleteSchema(String namespace, String group, int schemaId) {
        log.debug("Group {} {}, delete schema for version {} .", namespace, group, schemaId);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.deleteSchema(namespace, group, schemaId, etag)
                                                      .whenComplete((r, e) -> {
                                                          if (e == null) {
                                                              log.debug("Group {} {}, schema for verison {} deleted.", namespace, group, schemaId);
                                                          } else {
                                                              log.warn("Group {} {}, get schema version {} failed with error", namespace, group, schemaId, e);
                                                          }
                                                      })), executor);
    }

    /**
     * Delete schema corresponding to the version.
     *
     * @param namespace  namespace for which the request is scoped to.
     * @param group      Name of group.
     * @param schemaType schema type as specified in {@link SchemaInfo#type}
     * @param version    Version which uniquely identifies schema of schemaType within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<Void> deleteSchema(String namespace, String group, String schemaType, int version) {
        log.debug("Group {} {}, delete schema for version {}/{}.", namespace, group, schemaType, version);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.deleteSchema(namespace, group, schemaType, version, etag)
                                                      .whenComplete((r, e) -> {
                                                          if (e == null) {
                                                              log.debug("Group {} {}, schema for verison {}/{} deleted.", namespace, group, schemaType, version);
                                                          } else {
                                                              log.warn("Group {} {}, get schema version {}/{} failed with error", namespace, group, schemaType, version, e);
                                                          }
                                                      })), executor);
    }

    /**
     * Gets encoding info against the requested encoding Id.
     * Encoding Info uniquely identifies a combination of a schemaInfo and codecType.
     *
     * @param namespace  namespace for which the request is scoped to.
     * @param group      Name of group.
     * @param encodingId Encoding id that uniquely identifies a schema within a group.
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id.
     */
    public CompletableFuture<EncodingInfo> getEncodingInfo(String namespace, String group, EncodingId encodingId) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(encodingId != null);

        log.debug("Group {} {}, getEncodingInfo {} .", namespace, group, encodingId);

        return store.getEncodingInfo(namespace, group, encodingId)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, return getEncodingInfo {} {}.", namespace, group, r.getVersionInfo(), r.getCodecType());
                        } else {
                            log.warn("Group {} {}, get getEncodingInfo for id {} failed with error", namespace, group, encodingId, e);
                        }
                    });
    }

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and codec type.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @param version   version of schema
     * @param codecType codec type
     * @return CompletableFuture that holds Encoding id for the pair of version and codec type.
     */
    public CompletableFuture<EncodingId> getEncodingId(String namespace, String group, VersionInfo version, String codecType) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(version != null);
        Preconditions.checkArgument(codecType != null);
        log.debug("Group {} {}, getEncodingId for {} {}.", namespace, group, version, codecType);

        return RETRY.runAsync(() -> {
            return store.getEncodingId(namespace, group, version, codecType)
                        .thenCompose(response -> {
                            if (response.isLeft()) {
                                return CompletableFuture.completedFuture(response.getLeft());
                            } else {
                                return store.createEncodingId(namespace, group, version, codecType, response.getRight());
                            }
                        });
        }, executor)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, getEncodingId for {} {}. returning {}.", namespace, group, version, codecType, r);
                        } else {
                            log.warn("Group {} {}, getEncodingId for {} {} failed with error", namespace, group, version, codecType, e);
                        }
                    });
    }

    /**
     * Gets all schemas with corresponding versions for the group (or type, if specified).
     * If type is not specified all schemas with their respective versions in the group are listed.
     * Otherwise, only schema versions for the schema identified by type are listed.
     * The order in the list matches the order in which schemas were evolved within the group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @param type      Object type identified by {@link SchemaInfo#type}.
     * @return CompletableFuture that holds Ordered list of schemas with versions and compatibility for all schemas in the group.
     */
    public CompletableFuture<List<GroupHistoryRecord>> getGroupHistory(String namespace, String group, @Nullable String type) {
        Preconditions.checkArgument(group != null);
        log.debug("Group {} {}, getGroupHistory for {}.", namespace, group, type);

        if (type != null) {
            return store.getGroupHistoryForType(namespace, group, type)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.debug("Group {} {}, object type = {}, history size = {}.", namespace, group, type, r.size());
                            } else {
                                log.warn("Group {} {}, object type = {}, getGroupHistory failed with error", namespace, group, type, e);
                            }
                        });
        } else {
            return store.getGroupHistory(namespace, group)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.debug("Group {} {}, history size = {}.", namespace, group, r.size());
                            } else {
                                log.warn("Group {} {}, getGroupHistory failed with error", namespace, group, e);
                            }
                        });

        }
    }

    /**
     * Gets version corresponding to the schema.
     * For each unique {@link SchemaInfo#schemaData}, there will be a unique monotonically increasing version assigned.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @param schemaInfo    SchemaInfo that captures format and structure of the data.
     * @return CompletableFuture that holds VersionInfo corresponding to schema.
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schemaInfo) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schemaInfo != null);
        log.debug("Group {} {}, getSchemaVersion for {}.", namespace, group, schemaInfo.getType());
        SchemaInfo schema = normalizeSchemaBinary(schemaInfo);

        return store.getSchemaVersion(namespace, group, schema)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, version = {}.", namespace, group, r);
                        } else {
                            log.warn("Group {} {}, getSchemaVersion failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Checks whether given schema is valid by applying compatibility against previous schemas in the group
     * subject to current {@link GroupProperties#compatibility} policy.
     * Optionally a compatibility can be specified to specifically check against that policy. 
     * If {@link GroupProperties#allowMultipleTypes} is set, the validation is performed against schemas with same
     * object type identified by {@link SchemaInfo#type}.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @param schemaInfo    Schema to check for validity.
     * @param compatibility Optional compatibility to use. 
     * @return True if it satisfies validation checks, false otherwise.
     */
    public CompletableFuture<Boolean> validateSchema(String namespace, String group, SchemaInfo schemaInfo, Compatibility compatibility) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schemaInfo != null);
        log.debug("Group {} {}, validateSchema for {}.", namespace, group, schemaInfo.getType());
        SchemaInfo schema = normalizeSchemaBinary(schemaInfo);

        return store.getGroupProperties(namespace, group)
                    .thenCompose(prop -> {
                        if (!prop.getSerializationFormat().equals(SerializationFormat.Any) &&
                                !schema.getSerializationFormat().equals(prop.getSerializationFormat())) {
                            throw new SerializationFormatMismatchException(schema.getSerializationFormat().name());
                        }

                        GroupProperties toApply = new GroupProperties(prop.getSerializationFormat(), 
                                compatibility == null ? prop.getCompatibility() : compatibility,
                                prop.isAllowMultipleTypes(), prop.getProperties());
                        return getSchemasForValidation(namespace, group, schema, toApply)
                                .thenApply(schemas -> checkCompatibility(schema, toApply, schemas));
                    })
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, validateSchema response = {}.", namespace, group, r);
                        } else {
                            log.warn("Group {} {}, validateSchema failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Checks whether given schema can be used to read data written by schemas active in the group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of the group.
     * @param schemaInfo    Schema to check for ability to read.
     * @return True if schema can be used for reading subject to compatibility policy, false otherwise.
     */
    public CompletableFuture<Boolean> canRead(String namespace, String group, SchemaInfo schemaInfo) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schemaInfo != null);
        log.debug("Group {} {}, canRead for {}.", namespace, group, schemaInfo.getType());

        SchemaInfo schema = normalizeSchemaBinary(schemaInfo);
        return store.getGroupProperties(namespace, group)
                    .thenCompose(prop -> getSchemasForValidation(namespace, group, schema, prop)
                            .thenApply(schemasWithVersion -> canReadChecker(schema, prop, schemasWithVersion)))
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, canRead response = {}.", namespace, group, r);
                        } else {
                            log.warn("Group {} {}, canRead failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Deletes group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @return CompletableFuture which is completed when group is deleted.
     */
    public CompletableFuture<Void> deleteGroup(String namespace, String group) {
        Preconditions.checkArgument(group != null);
        log.debug("Group {} {}, deleteGroup.", namespace, group);

        return store.deleteGroup(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, group deleted", namespace, group);
                        } else {
                            log.warn("Group {} {}, group delete failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * List of compressions used for encoding in the group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of group.
     * @return CompletableFuture that holds list of compressions used for encoding in the group.
     */
    public CompletableFuture<List<CodecType>> getCodecTypes(String namespace, String group) {
        Preconditions.checkArgument(group != null);
        log.debug("Group {} {}, getCodecTypes.", namespace, group);

        return store.listCodecTypes(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, codecTypes = {}", namespace, group, r);
                        } else {
                            log.warn("Group {} {}, getcodecTypes failed with error", namespace, group, e);
                        }
                    });
    }

    /**
     * Method to add codecType to a group. Adding codecType is idempotent. CodecTypes need to be registered before
     * using them in encoding id related Apis.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     group.
     * @param codecType codec type to add.
     * @return CompletableFuture which when completed successfully will indicate successful addition of codecType to group.
     */
    public CompletableFuture<Void> addCodecType(String namespace, String group, CodecType codecType) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(codecType != null);

        log.debug("Group {} {}, addCodecType {}.", namespace, group, codecType);

        return store.addCodecType(namespace, group, codecType)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.debug("Group {} {}, addCodecType {} successful", namespace, group, codecType);
                        } else {
                            log.warn("Group {} {}, addCodecType {} failed with error", namespace, group, codecType, e);
                        }
                    });

    }

    private boolean isValidCompatibilityForFormat(SerializationFormat serializationFormat, Compatibility compatibility) {
        switch (serializationFormat) {
            case Avro:
                return true;
            case Protobuf:
                // Only Allow Any or Deny All are allowed values. 
            case Json:
                // Only Allow Any or Deny All are allowed values. 
            case Custom:
                // Only Allow Any or Deny All are allowed values. 
            case Any:
                return compatibility.getType().equals(Compatibility.Type.AllowAny) || compatibility.getType().equals(Compatibility.Type.DenyAll);
            default:
                throw new IllegalArgumentException("Unknown serialization format");
        }
    }

    private CompletableFuture<List<SchemaWithVersion>> getSchemasForValidation(String namespace, String group, SchemaInfo schema, 
                                                                               GroupProperties groupProperties) {
        switch (groupProperties.getCompatibility().getType()) {
            case AllowAny:
                return CompletableFuture.completedFuture(Collections.emptyList());
            case DenyAll:
                // Deny all is applicable as long as there is at least one schema in the group. 
                return store.listLatestSchemas(namespace, group);
            case Backward:
            case Forward:
            case BackwardTransitive:
            case ForwardTransitive:
            case Full:
            case FullTransitive:
            case Advanced:
                return getSchemasForBackwardAndForwardPolicy(namespace, group, schema, groupProperties);
            default:
                throw new IllegalArgumentException("Unknown Compatibility policy");
        }
    }

    private CompletableFuture<List<SchemaWithVersion>> getSchemasForBackwardAndForwardPolicy(String namespace, String group, SchemaInfo schema, GroupProperties groupProperties) {
        CompletableFuture<List<SchemaWithVersion>> schemasFuture;

        BackwardAndForward backwardAndForward = convertToBackwardAndForward(groupProperties.getCompatibility());
        BackwardPolicy backward = backwardAndForward.getBackwardPolicy();
        ForwardPolicy forward = backwardAndForward.getForwardPolicy();

        boolean fetchAll = backward instanceof BackwardTransitive || forward instanceof ForwardTransitive;

        if (fetchAll) {
            if (groupProperties.isAllowMultipleTypes()) {
                schemasFuture = store.listSchemasByType(namespace, group, schema.getType());
            } else {
                schemasFuture = store.listSchemas(namespace, group);
            }
        } else {
            int backwardTill = backward instanceof BackwardTill ? ((BackwardTill) backward).getVersionInfo().getId() : Integer.MAX_VALUE;  
            int forwardTill = forward instanceof ForwardTill ? ((ForwardTill) forward).getVersionInfo().getId() : Integer.MAX_VALUE;
            
            if (backwardTill != Integer.MAX_VALUE || forwardTill != Integer.MAX_VALUE) {
                VersionInfo till = backwardTill < forwardTill ? ((BackwardTill) backward).getVersionInfo() : ((ForwardTill) forward).getVersionInfo();  
                if (groupProperties.isAllowMultipleTypes()) {
                    schemasFuture = store.listSchemasByType(namespace, group, schema.getType(), till);
                } else {
                    schemasFuture = store.listSchemas(namespace, group, till);
                }
            } else {
                if (groupProperties.isAllowMultipleTypes()) {
                    schemasFuture = store.getLatestSchemaVersion(namespace, group, schema.getType())
                                         .thenApply(x -> x == null ? Collections.emptyList() : Collections.singletonList(x));
                } else {
                    schemasFuture = store.getLatestSchemaVersion(namespace, group)
                                         .thenApply(x -> x == null ? Collections.emptyList() : Collections.singletonList(x));
                }
            }
        }

        return schemasFuture;
    }

    private BackwardAndForward convertToBackwardAndForward(Compatibility compatibility) {
        BackwardAndForward.BackwardPolicy backwardPolicy = null;
        BackwardAndForward.ForwardPolicy forwardPolicy = null;

        switch (compatibility.getType()) {
            case Advanced:
                return compatibility.getBackwardAndForward();
            case Backward:
                backwardPolicy = BackwardAndForward.BACKWARD;
                break;
            case BackwardTransitive:
                backwardPolicy = BackwardAndForward.BACKWARD_TRANSITIVE;
                break;
            case Forward:
                forwardPolicy = BackwardAndForward.FORWARD;
                break;
            case ForwardTransitive:
                forwardPolicy = BackwardAndForward.FORWARD_TRANSITIVE;
                break;
            case Full:
                backwardPolicy = BackwardAndForward.BACKWARD;
                forwardPolicy = BackwardAndForward.FORWARD;
                break;
            case FullTransitive:
                backwardPolicy = BackwardAndForward.BACKWARD_TRANSITIVE;
                forwardPolicy = BackwardAndForward.FORWARD_TRANSITIVE;
                break;
            default:
                throw new IllegalArgumentException("Unknown compatibility policy");
        }
        return BackwardAndForward.builder().backwardPolicy(backwardPolicy).forwardPolicy(forwardPolicy).build();
    }
    
    private boolean checkCompatibility(SchemaInfo schema, GroupProperties groupProperties,
                                       List<SchemaWithVersion> schemasWithVersion) {
        CompatibilityChecker checker = CompatibilityCheckerFactory.getCompatibilityChecker(schema.getSerializationFormat());

        // Verify that the type matches the type in schemas it will be validated against.
        if (!schemasWithVersion.stream().allMatch(x -> x.getSchemaInfo().getType().equals(schema.getType()))) {
            return false;
        }
        switch (groupProperties.getCompatibility().getType()) {
            case AllowAny:
                return true;
            case DenyAll:
                return schemasWithVersion.isEmpty();
            case Backward:
            case Forward:
            case BackwardTransitive:
            case ForwardTransitive:
            case Full:
            case FullTransitive:
            case Advanced:
                BackwardAndForward backwardAndForward = convertToBackwardAndForward(groupProperties.getCompatibility());
                BackwardPolicy backward = backwardAndForward.getBackwardPolicy();
                ForwardPolicy forward = backwardAndForward.getForwardPolicy();
                boolean isValid = true;
                if (backward != null) {
                    List<SchemaInfo> schemas;
                    if (backward instanceof BackwardTill) {
                        schemas = schemasWithVersion.stream()
                                              .filter(x -> x.getVersionInfo().getVersion() >= ((BackwardTill) backward).getVersionInfo().getVersion())
                                              .map(SchemaWithVersion::getSchemaInfo)
                                              .collect(Collectors.toList());
                    } else {
                        schemas = schemasWithVersion.stream().map(SchemaWithVersion::getSchemaInfo).collect(Collectors.toList());
                    }
                    Collections.reverse(schemas);

                    isValid = checker.canRead(schema, schemas);
                } 
                if (isValid && forward != null) {
                    List<SchemaInfo> schemas;
                    if (forward instanceof ForwardTill) {
                        schemas = schemasWithVersion.stream()
                                              .filter(x -> x.getVersionInfo().getVersion() >= ((ForwardTill) forward).getVersionInfo().getVersion())
                                              .map(SchemaWithVersion::getSchemaInfo)
                                              .collect(Collectors.toList());
                    } else {
                        schemas = schemasWithVersion.stream().map(SchemaWithVersion::getSchemaInfo).collect(Collectors.toList());
                    }
                    Collections.reverse(schemas);

                    isValid = checker.canBeRead(schema, schemas);
                } 
                return isValid;
            default:
                throw new IllegalArgumentException("Unknown compatibility policy");
        }
    }

    private SchemaInfo normalizeSchemaBinary(SchemaInfo schemaInfo) {
        // validates and the schema binary. 
        ByteBuffer schemaBinary = schemaInfo.getSchemaData();
        boolean isValid = true;
        String invalidityCause = "";
        try {
            String schemaString;
            switch (schemaInfo.getSerializationFormat()) {
                case Protobuf:
                    String[] tokens = NameUtil.extractNameAndQualifier(schemaInfo.getType());
                    String qualifier = tokens[1]; // this can be empty string if schema info had no qualifier
                    String name = tokens[0];
                    DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(
                            schemaInfo.getSchemaData());

                    isValid = fileDescriptorSet
                            .getFileList().stream()
                            .anyMatch(x -> {
                                boolean match;
                                if (x.getPackage() == null) {
                                    match = Strings.isNullOrEmpty(qualifier);
                                } else {
                                    match = x.getPackage().equals(qualifier);
                                }
                                return match && x.getMessageTypeList().stream().anyMatch(y -> y.getName().equals(name));
                            });
                    if (!isValid) {
                        invalidityCause = "Type mismatch. Type should be full name for protobuf message. Hint: package.messageName";
                    } else {
                        schemaBinary = ByteBuffer.wrap(fileDescriptorSet.toByteArray());
                    }
                    break;
                case Avro:
                    schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                    Schema schema = new Schema.Parser().parse(schemaString);
                    if (schema.isUnion()) {
                        // get the schema for the type from the union
                        Optional<Schema> s = schema.getTypes().stream().filter(x -> x.getFullName().equals(schemaInfo.getType())).findAny();
                        isValid = s.isPresent();
                        schemaBinary = ByteBuffer.wrap(schema.toString().getBytes(Charsets.UTF_8));
                    } else {
                        isValid = schema.getFullName().equals(schemaInfo.getType());
                        schemaBinary = ByteBuffer.wrap(schema.toString().getBytes(Charsets.UTF_8));
                    }
                    if (!isValid) {
                        invalidityCause = "Type mismatch. Type should be full name for avro message. Hint: namespace.recordname";
                    } 
                    break;
                case Json:
                    schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(schemaString);
                    if (DRAFT_04_SCHEMA.equals(jsonNode.get(SCHEMA_NODE).asText()) ||
                            DRAFT_06_SCHEMA.equals(jsonNode.get(SCHEMA_NODE).asText()) ||
                            DRAFT_07_SCHEMA.equals(jsonNode.get(SCHEMA_NODE).asText())) {
                        JSONObject rawSchema = new JSONObject(new JSONTokener(schemaString));
                        SchemaLoader.builder().useDefaults(true).draftV6Support().draftV7Support().schemaJson(rawSchema)
                                                                           .build().load().build();
                    } else {
                        JsonSchema jsonSchema = OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
                        schemaBinary = ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsString(jsonSchema).getBytes(Charsets.UTF_8));
                    }

                    break;
                case Any:
                    break;
                case Custom:
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.debug("unable to parse schema {}", e.getMessage());
            isValid = false;
            invalidityCause = "Unable to parse schema";
        }
        if (!isValid) {
            throw new IllegalArgumentException(invalidityCause);
        }
        return new SchemaInfo(schemaInfo.getType(), schemaInfo.getSerializationFormat(), schemaBinary, schemaInfo.getProperties());
    }

    private Boolean canReadChecker(SchemaInfo schema, GroupProperties prop, List<SchemaWithVersion> schemasWithVersion) {
        CompatibilityChecker checker = CompatibilityCheckerFactory.getCompatibilityChecker(schema.getSerializationFormat());

        List<SchemaInfo> schemas = schemasWithVersion.stream().map(SchemaWithVersion::getSchemaInfo)
                                                     .collect(Collectors.toList());
        Collections.reverse(schemas);

        switch (prop.getCompatibility().getType()) {
            case AllowAny:
                return true;
            case DenyAll:
                return !schemas.isEmpty() &&
                        checker.canRead(schema, Collections.singletonList(schemas.get(0)));
            case Backward:
            case Forward:
            case BackwardTransitive:
            case ForwardTransitive:
            case Full:
            case FullTransitive:
            case Advanced:
                BackwardAndForward backwardAndForward = convertToBackwardAndForward(prop.getCompatibility());
                BackwardPolicy backward = backwardAndForward.getBackwardPolicy();
                ForwardPolicy forward = backwardAndForward.getForwardPolicy();
                boolean canRead = true;
                if (backward != null) {
                    List<SchemaInfo> schemasToUse;
                    if (backward instanceof BackwardTill) {
                        schemasToUse = schemasWithVersion.stream()
                                                    .filter(x -> x.getVersionInfo().getVersion() >= ((BackwardTill) backward).getVersionInfo().getVersion())
                                                    .map(SchemaWithVersion::getSchemaInfo)
                                                    .collect(Collectors.toList());
                    } else {
                        schemasToUse = schemasWithVersion.stream().map(SchemaWithVersion::getSchemaInfo).collect(Collectors.toList());
                    }
                    Collections.reverse(schemasToUse);

                    canRead = checker.canRead(schema, schemasToUse);
                }
                if (canRead && forward != null) {
                    canRead = !schemas.isEmpty() &&
                            checker.canRead(schema, Collections.singletonList(schemas.get(0)));
                }
                return canRead;
            default:
                throw new IllegalArgumentException("Unknown compatibility policy");
        }
    }

    /**
     * Method to get all groups where the supplied schema is registered in.
     *
     * @param namespace  namespace for which the request is scoped to.
     * @param schemaInfo schema to look for.
     * @return Map of group id to version that identifies the schema in the group.
     */
    public CompletableFuture<Map<String, VersionInfo>> getSchemaReferences(String namespace, SchemaInfo schemaInfo) {
        SchemaInfo schema = normalizeSchemaBinary(schemaInfo);

        return store.getGroupsUsing(namespace, schema)
                    .thenCompose(groups -> Futures.allOfWithResults(
                            groups.stream().collect(Collectors.toMap(x -> x, x ->
                                    Futures.exceptionallyExpecting(store.getSchemaVersion(namespace, x, schema),
                                            e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, EMPTY_VERSION))))
                                                  .thenApply(result -> {
                                                      return result.entrySet().stream().filter(x -> !x.getValue().equals(EMPTY_VERSION))
                                                                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                                  }));
    }
}
