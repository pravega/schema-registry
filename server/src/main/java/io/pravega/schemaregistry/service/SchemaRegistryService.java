/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.service;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.IncompatibleSchemaException;
import io.pravega.schemaregistry.contract.exceptions.PreconditionFailedException;
import io.pravega.schemaregistry.contract.exceptions.SchemaTypeMismatchException;
import io.pravega.schemaregistry.rules.CompatibilityChecker;
import io.pravega.schemaregistry.rules.CompatibilityCheckerFactory;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Schema registry service backend.
 */
@Slf4j
public class SchemaRegistryService {
    private static final Retry.RetryAndThrowConditionally RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                       .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);

    private final SchemaStore store;

    private final ScheduledExecutorService executor;

    public SchemaRegistryService(SchemaStore store, ScheduledExecutorService executor) {
        this.store = store;
        this.executor = executor;
    }

    /**
     * Lists groups.
     *
     * @param continuationToken continuation token
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroups(String continuationToken) {
        log.info("List groups called");
        return store.listGroups(ContinuationToken.parse(continuationToken))
                    .thenCompose(reply -> {
                        ContinuationToken token = reply.getToken();
                        List<String> list = reply.getList();
                        return Futures.allOfWithResults(list.stream().collect(Collectors.toMap(x -> x, store::getGroupProperties)))
                                      .thenApply(groups -> {
                                          log.info("Returning groups {}", groups);
                                          return new MapWithToken<>(groups, token);
                                      });
                    });
    }

    /**
     * Creates new group. Idempotent behaviour. If group already exists, it returns false.
     *
     * @param group           Name of group.
     * @param groupProperties Group properties.
     * @return CompletableFuture which is completed when create group completes. True indicates this was
     * new group, false indicates it was an existing group.
     */
    public CompletableFuture<Boolean> createGroup(String group, GroupProperties groupProperties) {
        Preconditions.checkNotNull(groupProperties.getSchemaType());
        Preconditions.checkNotNull(groupProperties.getSchemaValidationRules());
        Preconditions.checkArgument(validateRules(groupProperties.getSchemaType(), groupProperties.getSchemaValidationRules()));
        log.info("create group called for {}", group);
        return store.createGroup(group, groupProperties)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            if (r) {
                                log.info("Group {} created successfully.", group);
                            } else {
                                log.info("Group {} exists.", group);
                            }
                        } else {
                            log.warn("create group {} request failed with error", e, group);
                        }
                    });
    }

    /**
     * Gets group's properties.
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#versionBySchemaName} that specifies if schemas are evolved by object type.
     * Object types are uniquely identified by {@link SchemaInfo#name}.
     * {@link GroupProperties#properties} properties for a group.
     *
     * @param group Name of group.
     * @return CompletableFuture which holds group properties upon completion.
     */
    public CompletableFuture<GroupProperties> getGroupProperties(String group) {
        log.info("getGroupProperties called for group {}.", group);
        return store.getGroupProperties(group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {} properties.", group);
                        } else {
                            log.warn("getGroupProperties for group {} request failed with error", e, group);
                        }
                    });

    }

    /**
     * Update group's schema validation policy. If previous rules are sent, a conditional update is performed. 
     *
     * @param group           Name of group.
     * @param validationRules New validation rules for the group.
     * @param previousRules  Previous rules validation rules for the group. If null, unconditional update is performed.
     * @return CompletableFuture which is completed when validation policy update completes.
     */
    public CompletableFuture<Void> updateSchemaValidationRules(String group, SchemaValidationRules validationRules, 
                                                               SchemaValidationRules previousRules) {
        log.info("updateSchemaValidationRules called for group {}. New validation rules {}", group, validationRules);
        return RETRY.runAsync(() -> store.getGroupEtag(group)
                                .thenCompose(pos -> {
                                    if (previousRules == null) {
                                        return store.updateValidationRules(group, pos, validationRules);
                                    } else {
                                        return store.getGroupProperties(group)
                                             .thenCompose(prop -> {
                                                 if (previousRules.equals(prop.getSchemaValidationRules())) {
                                                     return store.updateValidationRules(group, pos, validationRules);
                                                 } else {
                                                     throw new PreconditionFailedException("Conditional update failed");
                                                 }
                                             });
                                    }
                                })
                                .whenComplete((r, e) -> {
                                    if (e == null) {
                                        log.info("Group {} updateSchemaValidationRules successful.", group);
                                    } else {
                                        log.warn("getGroupProperties for group {} request failed with error", e, group);
                                    }
                                }), executor);
    }

    /**
     * Gets list of object types registered under the group. Object types are identified by {@link SchemaInfo#name}
     *
     * @param group Name of group.
     * @param token Continuation token.
     * @return CompletableFuture which holds list of object types upon completion.
     */
    public CompletableFuture<ListWithToken<String>> getObjectTypes(String group, ContinuationToken token) {
        log.info("getObjectTypes called for group {}. New validation rules {}", group);

        return store.listObjectTypes(group, token)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {} getObjectTypes {}.", group, r.getList());
                        } else {
                            log.warn("getObjectTypes for group {} request failed with error", e, group);
                        }
                    });
    }

    /**
     * Adds schema to the group. If group is configured with {@link GroupProperties#versionBySchemaName}, then
     * the {@link SchemaInfo#name} is used to filter previous schemas and apply schema validation policy against schemas
     * of object type.
     * Schema validation rules that are sent to the registry should be a super set of Validation rules set in
     * {@link GroupProperties#schemaValidationRules}
     *
     * @param group  Name of group.
     * @param schema Schema to add.
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.
     */
    public CompletableFuture<VersionInfo> addSchema(String group, SchemaInfo schema) {
        log.info("addSchema called for group {}. schema {}", schema.getName());

        // 1. get group policy
        // 2. get checker for schema type.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        return RETRY.runAsync(() -> store.getGroupEtag(group)
                    .thenCompose(etag ->
                            store.getGroupProperties(group)
                                 .thenCompose(prop -> {
                                     if (!schema.getSchemaType().equals(prop.getSchemaType()) && !prop.getSchemaType().equals(SchemaType.Any)) {
                                         throw new SchemaTypeMismatchException(schema.getSchemaType().name());
                                     }
                                     return Futures.exceptionallyComposeExpecting(store.getSchemaVersion(group, schema),
                                             e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                                             () -> { // Schema doesnt exist. Validate and add it
                                                 return getSchemasForValidation(group, schema, prop)
                                                         .thenApply(schemas -> checkCompatibility(schema, prop, schemas))
                                                         .thenCompose(valid -> {
                                                             if (!valid) {
                                                                 throw new IncompatibleSchemaException(String.format("%s is incomatible", schema.getName()));
                                                             }
                                                             return store.addSchemaToGroup(group, schema, prop, etag);
                                                         });
                                             });
                                 })), executor)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, schema {} added successfully.", group, schema.getName());
                        } else {
                            log.warn("Group {}, schema {} add failed with error", e, group);
                        }
                    });
    }

    /**
     * Gets schema corresponding to the version.
     *
     * @param group   Name of group.
     * @param versionOrdinal Version which uniquely identifies schema within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<SchemaInfo> getSchema(String group, int versionOrdinal) {
        log.info("Group {}, get schema for version {} .", group, versionOrdinal);

        return store.getSchema(group, versionOrdinal)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, return schema for verison {}.", group, versionOrdinal);
                        } else {
                            log.warn("Group {}, get schema version {} failed with error", e, group, versionOrdinal);
                        }
                    });
    }

    /**
     * Gets encoding info against the requested encoding Id.
     * Encoding Info uniquely identifies a combination of a schemaInfo and codecType.
     *
     * @param group      Name of group.
     * @param encodingId Encoding id that uniquely identifies a schema within a group.
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id.
     */
    public CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId) {
        log.info("Group {}, getEncodingInfo {} .", group, encodingId);

        return store.getEncodingInfo(group, encodingId)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, return getEncodingInfo {} {}.", group, r.getVersionInfo(), r.getCodec());
                        } else {
                            log.warn("Group {}, get getEncodingInfo for id {} failed with error", e, group, encodingId);
                        }
                    });
    }

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and codec type.
     *
     * @param group     Name of group.
     * @param version   version of schema
     * @param codecType codec type
     * @return CompletableFuture that holds Encoding id for the pair of version and codec type.
     */
    public CompletableFuture<EncodingId> getEncodingId(String group, VersionInfo version, CodecType codecType) {
        log.info("Group {}, getEncodingId for {} {}.", group, version, codecType);

        return RETRY.runAsync(() -> {
            return store.getEncodingId(group, version, codecType)
                        .thenCompose(response -> {
                            if (response.isLeft()) {
                                return CompletableFuture.completedFuture(response.getLeft());
                            } else {
                                return store.createEncodingId(group, version, codecType, response.getRight());
                            }
                        });
        }, executor)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, getEncodingId for {} {}. returning {}.", group, version, codecType, r);
                        } else {
                            log.warn("Group {}, getEncodingId for {} {} failed with error", e, group, version, codecType);
                        }
                    });
    }

    /**
     * Gets latest schema and version for the group (or objectType, if specified).
     * For groups configured with {@link GroupProperties#versionBySchemaName}, the objectTypename needs to be supplied to
     * get the latest schema for the object type.
     *
     * @param group          Name of group.
     * @param objectTypeName Object type.
     * @return CompletableFuture that holds Schema with version for the last schema that was added to the group.
     */
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group, @Nullable String objectTypeName) {
        log.info("Group {}, getLatestSchema for {}.", group, objectTypeName);

        if (objectTypeName == null) {
            return store.getLatestSchema(group)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, getLatestSchema = {}.", group, r.getVersion());
                            } else {
                                log.warn("Group {}, getLatestSchema failed with error", e, group);
                            }
                        });
        } else {
            return store.getLatestSchema(group, objectTypeName)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, object type = {}, getLatestSchema = {}.", group, objectTypeName, r.getVersion());
                            } else {
                                log.warn("Group {}, object type = {}, getLatestSchema failed with error", e, group, objectTypeName);
                            }
                        });
        }
    }

    /**
     * Gets all schemas with corresponding versions for the group (or objectTypeName, if specified).
     * For groups configured with {@link GroupProperties#versionBySchemaName}, the objectTypeName name needs to be supplied to
     * get the latest schema for the objectTypeName. {@link SchemaInfo#name} is used as the objectTypeName name.
     * The order in the list matches the order in which schemas were evolved within the group.
     *
     * @param group          Name of group.
     * @param objectTypeName Object type.
     * @return CompletableFuture that holds Ordered list of schemas with versions and validation rules for all schemas in the group.
     */
    public CompletableFuture<List<SchemaEvolution>> getGroupEvolutionHistory(String group, @Nullable String objectTypeName) {
        log.info("Group {}, getGroupEvolutionHistory for {}.", group, objectTypeName);

        if (objectTypeName != null) {
            return store.getGroupHistoryForObjectType(group, objectTypeName).thenApply(ListWithToken::getList)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, object type = {}, history size = {}.", group, objectTypeName, r.size());
                            } else {
                                log.warn("Group {}, object type = {}, getLatestSchema failed with error", e, group, objectTypeName);
                            }
                        });
        } else {
            return store.getGroupHistory(group).thenApply(ListWithToken::getList)
                        .whenComplete((r, e) -> {
                            if (e == null) {
                                log.info("Group {}, history size = {}.", group, r.size());
                            } else {
                                log.warn("Group {}, getLatestSchema failed with error", e, group);
                            }
                        });

        }
    }

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#versionBySchemaName}
     * the objectTypename is taken from the {@link SchemaInfo#name}.
     * For each unique {@link SchemaInfo#schemaData}, there will be a unique monotonically increasing version assigned.
     *
     * @param group  Name of group.
     * @param schema SchemaInfo that captures schema name and schema data.
     * @return CompletableFuture that holds VersionInfo corresponding to schema.
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schema) {
        log.info("Group {}, getSchemaVersion for {}.", group, schema.getName());

        return store.getSchemaVersion(group, schema)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, version = {}.", group, r);
                        } else {
                            log.warn("Group {}, getSchemaVersion failed with error", e, group);
                        }
                    });
    }

    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * If {@link GroupProperties#versionBySchemaName} is set, the validation is performed against schemas with same
     * object type identified by {@link SchemaInfo#name}.
     *
     * @param group  Name of group.
     * @param schema Schema to check for validity.
     * @return True if it satisfies validation checks, false otherwise.
     */
    public CompletableFuture<Boolean> validateSchema(String group, SchemaInfo schema) {
        log.info("Group {}, validateSchema for {}.", group, schema.getName());

        return store.getGroupProperties(group)
                    .thenCompose(prop -> getSchemasForValidation(group, schema, prop)
                            .thenApply(schemas -> checkCompatibility(schema, prop, schemas)))
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, validateSchema response = {}.", group, r);
                        } else {
                            log.warn("Group {}, validateSchema failed with error", e, group);
                        }
                    });
    }

    /**
     * Checks whether given schema can be used to read data written by schemas active in the group.
     *
     * @param group  Name of the group.
     * @param schema Schema to check for ability to read.
     * @return True if schema can be used for reading subject to compatibility policy, false otherwise.
     */
    public CompletableFuture<Boolean> canRead(String group, SchemaInfo schema) {
        log.info("Group {}, canRead for {}.", group, schema.getName());

        return store.getGroupProperties(group)
                    .thenCompose(prop -> getSchemasForValidation(group, schema, prop)
                            .thenApply(schemasWithVersion -> canReadChecker(schema, prop, schemasWithVersion)))
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, canRead response = {}.", group, r);
                        } else {
                            log.warn("Group {}, canRead failed with error", e, group);
                        }
                    });
    }

    /**
     * Deletes group.
     *
     * @param group Name of group.
     * @return CompletableFuture which is completed when group is deleted.
     */
    public CompletableFuture<Void> deleteGroup(String group) {
        log.info("Group {}, deleteGroup.", group);

        return store.deleteGroup(group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, group deleted", group);
                        } else {
                            log.warn("Group {}, group delete failed with error", e, group);
                        }
                    });
    }

    /**
     * List of compressions used for encoding in the group.
     *
     * @param group Name of group.
     * @return CompletableFuture that holds list of compressions used for encoding in the group.
     */
    public CompletableFuture<List<CodecType>> getCodecTypes(String group) {
        log.info("Group {}, getCodecTypes.", group);

        return store.getCodecTypes(group)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, codecs = {}", group, r);
                        } else {
                            log.warn("Group {}, getcodecs failed with error", e, group);
                        }
                    });
    }

    public CompletableFuture<Void> addCodec(String group, CodecType codecType) {
        log.info("Group {}, addCodec {}.", group, codecType);

        return store.addCodec(group, codecType)
                    .whenComplete((r, e) -> {
                        if (e == null) {
                            log.info("Group {}, addCodec {} successful", group, codecType);
                        } else {
                            log.warn("Group {}, addCodec {} failed with error", e, group, codecType);
                        }
                    });

    }
    
    private boolean validateRules(SchemaType schemaType, SchemaValidationRules newRules) {
        Preconditions.checkNotNull(newRules);

        switch (schemaType) {
            case Avro:
                return true;
            case Protobuf:
            case Json:
            case Custom:
                return newRules.getRules().size() == 1 &&
                        newRules.getRules().entrySet().stream().allMatch(x -> {
                            return x.getValue() instanceof Compatibility && (
                                    ((Compatibility) x.getValue()).getCompatibility().equals(Compatibility.Type.AllowAny) ||
                                            ((Compatibility) x.getValue()).getCompatibility().equals(Compatibility.Type.DenyAll));
                        });
        }
        return true;
    }

    private CompletableFuture<List<SchemaWithVersion>> getSchemasForValidation(String group, SchemaInfo schema, GroupProperties groupProperties) {
        CompletableFuture<List<SchemaWithVersion>> schemasFuture;
        boolean fetchAll = groupProperties.getSchemaValidationRules().getRules().values().stream()
                                          .anyMatch(x -> x instanceof Compatibility
                                                  && (((Compatibility) x).getCompatibility().equals(Compatibility.Type.BackwardTransitive)
                                                  || ((Compatibility) x).getCompatibility().equals(Compatibility.Type.ForwardTransitive)
                                                  || ((Compatibility) x).getCompatibility().equals(Compatibility.Type.FullTransitive)));

        if (fetchAll) {
            if (groupProperties.isVersionBySchemaName()) {
                schemasFuture = store.listSchemasByObjectType(group, schema.getName(), null)
                                     .thenApply(ListWithToken::getList);
            } else {
                schemasFuture = store.listSchemas(group, null)
                                     .thenApply(ListWithToken::getList);
            }
        } else {
            VersionInfo till = groupProperties.getSchemaValidationRules().getRules().values().stream()
                                              .filter(x -> x instanceof Compatibility
                                                      && (((Compatibility) x).getCompatibility().equals(Compatibility.Type.BackwardTill)
                                                      || ((Compatibility) x).getCompatibility().equals(Compatibility.Type.ForwardTill)
                                                      || ((Compatibility) x).getCompatibility().equals(Compatibility.Type.BackwardAndForwardTill)))
                                              .map(x -> {
                                                  Compatibility compatibility = (Compatibility) x;
                                                  if (compatibility.getCompatibility().equals(Compatibility.Type.BackwardTill)) {
                                                      return compatibility.getBackwardTill();
                                                  } else if (compatibility.getCompatibility().equals(Compatibility.Type.ForwardTill)) {
                                                      return compatibility.getForwardTill();
                                                  } else {
                                                      return compatibility.getBackwardTill().getVersion() < compatibility.getForwardTill().getVersion() ?
                                                              compatibility.getBackwardTill() : compatibility.getForwardTill();
                                                  }
                                              }).max(Comparator.comparingInt(VersionInfo::getVersion)).orElse(null);
            if (till != null) {
                if (groupProperties.isVersionBySchemaName()) {
                    schemasFuture = store.listSchemasByObjectType(group, schema.getName(), till, null)
                                         .thenApply(ListWithToken::getList);
                } else {
                    schemasFuture = store.listSchemas(group, till, null)
                                         .thenApply(ListWithToken::getList);
                }
            } else {
                if (groupProperties.isVersionBySchemaName()) {
                    schemasFuture = store.getLatestSchema(group, schema.getName())
                                         .thenApply(x -> x == null ? Collections.emptyList() : Collections.singletonList(x));
                } else {
                    schemasFuture = store.getLatestSchema(group)
                                         .thenApply(x -> x == null ? Collections.emptyList() : Collections.singletonList(x));
                }
            }
        }

        return schemasFuture;
    }

    private boolean checkCompatibility(SchemaInfo schema, GroupProperties groupProperties,
                                       List<SchemaWithVersion> schemasWithVersion) {
        CompatibilityChecker checker = CompatibilityCheckerFactory.getCompatibilityChecker(schema.getSchemaType());

        List<SchemaInfo> schemas = schemasWithVersion.stream().map(SchemaWithVersion::getSchema).collect(Collectors.toList());
        Collections.reverse(schemas);
        for (SchemaValidationRule rule : groupProperties.getSchemaValidationRules().getRules().values()) {
            if (rule instanceof Compatibility) {
                Compatibility compatibility = (Compatibility) rule;
                boolean isValid;
                switch (compatibility.getCompatibility()) {
                    case Backward:
                    case BackwardTill:
                    case BackwardTransitive:
                        isValid = checker.canRead(schema, schemas);
                        break;
                    case Forward:
                    case ForwardTill:
                    case ForwardTransitive:
                        isValid = checker.canBeRead(schema, schemas);
                        break;
                    case Full:
                    case FullTransitive:
                        isValid = checker.canMutuallyRead(schema, schemas);
                        break;
                    case BackwardAndForwardTill:
                        List<SchemaInfo> backwardTillList = new LinkedList<>();
                        List<SchemaInfo> forwardTillList = new LinkedList<>();
                        schemasWithVersion.forEach(x -> {
                            if (x.getVersion().getVersion() >= compatibility.getBackwardTill().getVersion()) {
                                backwardTillList.add(x.getSchema());
                            }
                            if (x.getVersion().getVersion() >= compatibility.getForwardTill().getVersion()) {
                                forwardTillList.add(x.getSchema());
                            }
                        });
                        isValid = checker.canRead(schema, backwardTillList) & checker.canBeRead(schema, forwardTillList);
                        break;
                    case AllowAny:
                        isValid = true;
                        break;
                    case DenyAll:
                    default:
                        isValid = schemasWithVersion.isEmpty();
                        break;
                }
                return isValid;
            }
        }
        // if no rules are set, we will come here. 
        return true;
    }

    private Boolean canReadChecker(SchemaInfo schema, GroupProperties prop, List<SchemaWithVersion> schemasWithVersion) {
        CompatibilityChecker checker = CompatibilityCheckerFactory.getCompatibilityChecker(schema.getSchemaType());

        List<SchemaInfo> schemas = schemasWithVersion.stream().map(SchemaWithVersion::getSchema)
                                                     .collect(Collectors.toList());
        Collections.reverse(schemas);
        for (SchemaValidationRule rule : prop.getSchemaValidationRules().getRules().values()) {
            if (rule instanceof Compatibility) {
                boolean canRead;
                Compatibility compatibility = (Compatibility) rule;
                switch (compatibility.getCompatibility()) {
                    case Backward:
                    case BackwardTill:
                    case BackwardTransitive:
                        canRead = checker.canRead(schema, schemas);
                        break;
                    case Forward:
                    case ForwardTill:
                    case ForwardTransitive:
                    case Full:
                    case DenyAll:
                        // check can read latest.
                        canRead = !schemas.isEmpty() &&
                                checker.canRead(schema, Collections.singletonList(schemas.get(0)));
                        break;
                    case FullTransitive:
                        canRead = checker.canRead(schema, schemas);
                        break;
                    case BackwardAndForwardTill:
                        List<SchemaInfo> backwardTillList = new LinkedList<>();
                        schemasWithVersion.forEach(x -> {
                            if (x.getVersion().getVersion() >= compatibility.getBackwardTill().getVersion()) {
                                backwardTillList.add(x.getSchema());
                            }
                        });
                        canRead = checker.canRead(schema, backwardTillList);
                        break;
                    case AllowAny:
                        canRead = true;
                        break;
                    default:
                        canRead = false;
                        break;
                }
                return canRead;
            }
        }
        // if no rules are set we will come here and return true always
        return true;
    }
}
