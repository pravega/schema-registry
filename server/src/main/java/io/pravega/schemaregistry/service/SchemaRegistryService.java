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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.common.FuturesCollector;
import io.pravega.schemaregistry.common.NameUtil;
import io.pravega.schemaregistry.contract.data.BackwardAndForward;
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

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.contract.data.BackwardAndForward.*;

/**
 * Schema registry service backend.
 */
@Slf4j
public class SchemaRegistryService {
    private static final Retry.RetryAndThrowConditionally RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                       .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final VersionInfo EMPTY_VERSION = new VersionInfo("", -1, -1);

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
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroups(String namespace, ContinuationToken continuationToken, int limit) {
        log.info("List groups called");
        return FuturesCollector.filteredWithTokenAndLimit(
                (ContinuationToken c, Integer l) -> store.listGroups(namespace, c, l)
                                                         .thenCompose(reply -> {
                                                             List<String> list = reply.getList();
                                                             return Futures.allOfWithResults(list.stream().map(x -> Futures.exceptionallyExpecting(store.getGroupProperties(namespace, x).thenApply(AtomicReference::new),
                                                                     e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                                                                     new AtomicReference<>((GroupProperties) null)).thenApply(prop -> new AbstractMap.SimpleEntry<>(x, prop)))
                                                                                                 .collect(Collectors.toList()))
                                                                           .thenApply(result -> new AbstractMap.SimpleEntry<>(reply.getToken(), result));
                                                         }),
                x -> x.getValue().get() != null, continuationToken, limit, executor)
                               .thenApply(groupsList -> {
                                   log.info("Returning groups {}", groupsList);
                                   Map<String, GroupProperties> collect = groupsList.getValue().stream().collect(
                                           Collectors.toMap(AbstractMap.SimpleEntry::getKey, x -> x.getValue().get()));
                                   return new MapWithToken<>(
                                           collect, groupsList.getKey());
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
        Preconditions.checkArgument(validateRules(groupProperties.getSerializationFormat(), groupProperties.getCompatibility()));
        log.info("create group called for {} with group properties {}", group, groupProperties);
        return store.createGroup(namespace, group, groupProperties)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            if (r) {
                                log.info("Group {} created successfully.", group);
                            } else {
                                log.info("Group {} exists.", group);
                            }
                        } else {
                            log.warn("create group {} request failed with error", group, e);
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
        log.info("getGroupProperties called for group {}.", group);
        return store.getGroupProperties(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {} properties.", group);
                        } else {
                            log.warn("getGroupProperties for group {} request failed with error", group, e);
                        }
                    });

    }

    /**
     * Update group's schema compatibility policy. If previous rules are sent, a conditional update is performed.
     *
     * @param namespace       namespace for which the request is scoped to.
     * @param group           Name of group.
     * @param compatibility New compatibility for the group.
     * @param previousRules   Previous rules compatibility for the group. If null, unconditional update is performed.
     * @return CompletableFuture which is completed when compatibility policy update completes.
     */
    public CompletableFuture<Void> updateCompatibility(String namespace, String group, Compatibility compatibility,
                                                       @Nullable Compatibility previousRules) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(compatibility != null);
        log.info("updateCompatibility called for group {}. New compatibility {}", group, compatibility);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(pos -> {
                                             return store.getGroupProperties(namespace, group)
                                                         .thenCompose(prop -> {
                                                             if (previousRules == null) {
                                                                 return store.updateCompatibility(namespace, group, pos, compatibility);
                                                             } else {
                                                                 if (previousRules.equals(prop.getCompatibility())) {
                                                                     return store.updateCompatibility(namespace, group, pos, compatibility);
                                                                 } else {
                                                                     throw new PreconditionFailedException("Conditional update failed");
                                                                 }
                                                             }
                                                         });
                                         })
                                         .whenComplete((r, e) -> {
                                             if (e == null) {
                                                 log.info("Group {} updateCompatibility successful.", group);
                                             } else {
                                                 log.warn("getGroupProperties for group {} request failed with error", group, e);
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
        log.info("getSchemas called for group {}. New compatibility {}", group);

        if (schemaType == null) {
            return store.getLatestSchemas(namespace, group)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {} getSchemas {}.", group, r);
                            } else {
                                log.warn("getSchemas for group {} request failed with error", group, e);
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
     * @param schema    Schema to add.
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.
     */
    public CompletableFuture<VersionInfo> addSchema(String namespace, String group, SchemaInfo schema) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schema != null);
        log.info("addSchema called for group {}. schema {}", schema.getType());

        // 1. get group policy
        // 2. get checker for serialization format.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.getGroupProperties(namespace, group)
                                                      .thenCompose(prop -> {
                                                          if (!schema.getSerializationFormat().equals(prop.getSerializationFormat()) &&
                                                                  !prop.getSerializationFormat().equals(SerializationFormat.Any)) {
                                                              throw new SerializationFormatMismatchException(schema.getSerializationFormat().name());
                                                          }
                                                          return Futures.exceptionallyComposeExpecting(store.getSchemaVersion(namespace, group, schema),
                                                                  e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                                                                  () -> { // Schema doesnt exist. Validate and add it
                                                                      return getSchemasForValidation(namespace, group, schema, prop)
                                                                              .thenApply(schemas -> checkCompatibility(schema, prop, schemas))
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
                            log.info("Group {}, schema {} added successfully.", group, schema.getType());
                        } else {
                            log.warn("Group {}, schema {} add failed with error", group, e);
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
        log.info("Group {}, get schema for version {} .", group, schemaId);

        return store.getSchema(namespace, group, schemaId)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, return schema for verison {}.", group, schemaId);
                        } else {
                            log.warn("Group {}, get schema version {} failed with error", group, schemaId, e);
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
        log.info("Group {}, get schema for version {}/{}.", group, schemaType, version);

        return store.getSchema(namespace, group, schemaType, version)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, return schema for verison {}/{}.", group, schemaType, version);
                        } else {
                            log.warn("Group {}, get schema version {}/{} failed with error", group, schemaType, version, e);
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
        log.info("Group {}, delete schema for version {} .", group, schemaId);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.deleteSchema(namespace, group, schemaId, etag)
                                                      .whenComplete((r, e) -> {
                                                          if (e == null) {
                                                              log.info("Group {}, schema for verison {} deleted.", group, schemaId);
                                                          } else {
                                                              log.warn("Group {}, get schema version {} failed with error", group, schemaId, e);
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
        log.info("Group {}, delete schema for version {}/{}.", group, schemaType, version);
        return RETRY.runAsync(() -> store.getGroupEtag(namespace, group)
                                         .thenCompose(etag ->
                                                 store.deleteSchema(namespace, group, schemaType, version, etag)
                                                      .whenComplete((r, e) -> {
                                                          if (e == null) {
                                                              log.info("Group {}, schema for verison {}/{} deleted.", group, schemaType, version);
                                                          } else {
                                                              log.warn("Group {}, get schema version {}/{} failed with error", group, schemaType, version, e);
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

        log.info("Group {}, getEncodingInfo {} .", group, encodingId);

        return store.getEncodingInfo(namespace, group, encodingId)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, return getEncodingInfo {} {}.", group, r.getVersionInfo(), r.getCodecType());
                        } else {
                            log.warn("Group {}, get getEncodingInfo for id {} failed with error", group, encodingId, e);
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
        log.info("Group {}, getEncodingId for {} {}.", group, version, codecType);

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
                            log.info("Group {}, getEncodingId for {} {}. returning {}.", group, version, codecType, r);
                        } else {
                            log.warn("Group {}, getEncodingId for {} {} failed with error", group, version, codecType, e);
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
        log.info("Group {}, getGroupHistory for {}.", group, type);

        if (type != null) {
            return store.getGroupHistoryForType(namespace, group, type)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, object type = {}, history size = {}.", group, type, r.size());
                            } else {
                                log.warn("Group {}, object type = {}, getGroupHistory failed with error", group, type, e);
                            }
                        });
        } else {
            return store.getGroupHistory(namespace, group)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, history size = {}.", group, r.size());
                            } else {
                                log.warn("Group {}, getGroupHistory failed with error", group, e);
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
     * @param schema    SchemaInfo that captures format and structure of the data.
     * @return CompletableFuture that holds VersionInfo corresponding to schema.
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String namespace, String group, SchemaInfo schema) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schema != null);
        log.info("Group {}, getSchemaVersion for {}.", group, schema.getType());

        return store.getSchemaVersion(namespace, group, schema)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, version = {}.", group, r);
                        } else {
                            log.warn("Group {}, getSchemaVersion failed with error", group, e);
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
     * @param schema    Schema to check for validity.
     * @param compatibility Optional compatibility to use. 
     * @return True if it satisfies validation checks, false otherwise.
     */
    public CompletableFuture<Boolean> validateSchema(String namespace, String group, SchemaInfo schema, Compatibility compatibility) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schema != null);
        log.info("Group {}, validateSchema for {}.", group, schema.getType());

        return store.getGroupProperties(namespace, group)
                    .thenCompose(prop -> {
                        GroupProperties toApply = new GroupProperties(prop.getSerializationFormat(), 
                                compatibility == null ? prop.getCompatibility() : compatibility,
                                prop.isAllowMultipleTypes(), prop.getProperties());
                        return getSchemasForValidation(namespace, group, schema, toApply)
                                .thenApply(schemas -> checkCompatibility(schema, toApply, schemas));
                    })
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, validateSchema response = {}.", group, r);
                        } else {
                            log.warn("Group {}, validateSchema failed with error", group, e);
                        }
                    });
    }

    /**
     * Checks whether given schema can be used to read data written by schemas active in the group.
     *
     * @param namespace namespace for which the request is scoped to.
     * @param group     Name of the group.
     * @param schema    Schema to check for ability to read.
     * @return True if schema can be used for reading subject to compatibility policy, false otherwise.
     */
    public CompletableFuture<Boolean> canRead(String namespace, String group, SchemaInfo schema) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(schema != null);
        log.info("Group {}, canRead for {}.", group, schema.getType());

        return store.getGroupProperties(namespace, group)
                    .thenCompose(prop -> getSchemasForValidation(namespace, group, schema, prop)
                            .thenApply(schemasWithVersion -> canReadChecker(schema, prop, schemasWithVersion)))
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, canRead response = {}.", group, r);
                        } else {
                            log.warn("Group {}, canRead failed with error", group, e);
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
        log.info("Group {}, deleteGroup.", group);

        return store.deleteGroup(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, group deleted", group);
                        } else {
                            log.warn("Group {}, group delete failed with error", group, e);
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
    public CompletableFuture<List<String>> getCodecTypes(String namespace, String group) {
        Preconditions.checkArgument(group != null);
        log.info("Group {}, getCodecTypes.", group);

        return store.getCodecTypes(namespace, group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, codecTypes = {}", group, r);
                        } else {
                            log.warn("Group {}, getcodecTypes failed with error", group, e);
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
    public CompletableFuture<Void> addCodecType(String namespace, String group, String codecType) {
        Preconditions.checkArgument(group != null);
        Preconditions.checkArgument(codecType != null);

        log.info("Group {}, addCodecType {}.", group, codecType);

        return store.addCodecType(namespace, group, codecType)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, addCodecType {} successful", group, codecType);
                        } else {
                            log.warn("Group {}, addCodecType {} failed with error", group, codecType, e);
                        }
                    });

    }

    private boolean validateRules(SerializationFormat serializationFormat, Compatibility newRules) {
        switch (serializationFormat) {
            case Avro:
                return true;
            case Protobuf:
            case Json:
            case Custom:
            case Any:
                return newRules.getType().equals(Compatibility.Type.AllowAny) || newRules.getType().equals(Compatibility.Type.DenyAll);
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
                return store.getLatestSchemas(namespace, group);
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
        validateSchemaData(schema);
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

    private void validateSchemaData(SchemaInfo schemaInfo) {
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
                    } 
                    break;
                case Avro:
                    schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                    Schema schema = new Schema.Parser().parse(schemaString);
                    
                    isValid = schema.getFullName().equals(schemaInfo.getType());
                    if (!isValid) {
                        invalidityCause = "Type mismatch. Type should be full name for avro message. Hint: namespace.recordname";
                    }

                    break;
                case Json:
                    schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                    OBJECT_MAPPER.readValue(schemaString, JsonSchema.class);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.info("unable to parse schema {}", e.getMessage());
            isValid = false;
            invalidityCause = "Unable to parse schema";
        }
        if (!isValid) {
            throw new IllegalArgumentException(invalidityCause);
        }
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
        return store.getGroupsUsing(namespace, schemaInfo)
                    .thenCompose(groups -> Futures.allOfWithResults(
                            groups.stream().collect(Collectors.toMap(x -> x, x ->
                                    Futures.exceptionallyExpecting(store.getSchemaVersion(namespace, x, schemaInfo),
                                            e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, EMPTY_VERSION))))
                                                  .thenApply(result -> {
                                                      return result.entrySet().stream().filter(x -> !x.getValue().equals(EMPTY_VERSION))
                                                                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                                  }));
    }
}
