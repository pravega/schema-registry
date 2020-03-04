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

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.MapWithToken;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.rules.CompatibilityChecker;
import io.pravega.schemaregistry.storage.ContinuationToken;
import io.pravega.schemaregistry.storage.SchemaStore;

import javax.annotation.Nullable;
import java.util.InputMismatchException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SchemaRegistryService {
    private final SchemaStore store;

    public SchemaRegistryService(SchemaStore store) {
        this.store = store;
    }

    /**
     * Lists groups.
     *
     * @param continuationToken continuation token
     * @return CompletableFuture which holds map of groups names and group properties upon completion.
     */
    public CompletableFuture<MapWithToken<String, GroupProperties>> listGroups(String continuationToken) {
        return store.listGroups(ContinuationToken.parse(continuationToken))
                    .thenCompose(reply -> {
                        ContinuationToken token = reply.getToken();
                        List<String> list = reply.getList();
                        return Futures.allOfWithResults(list.stream().collect(Collectors.toMap(x -> x, store::getGroupProperties)))
                                      .thenApply(groups -> new MapWithToken<>(groups, token));
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
        return store.createGroup(group, groupProperties);
    }

    /**
     * Gets group's properties.
     * {@link GroupProperties#schemaType} which identifies the serialization format and schema type used to describe the schema.
     * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
     * {@link GroupProperties#validateByEventType} that specifies if schemas are evolved by event type.
     * Event Types are uniquely identified by {@link SchemaInfo#name}.
     * {@link GroupProperties#enableEncoding} describes whether registry should generate encoding ids to identify
     * encoding properties in {@link EncodingInfo}.
     *
     * @param group Name of group.
     * @return CompletableFuture which holds group properties upon completion.
     */
    public CompletableFuture<GroupProperties> getGroupProperties(String group) {
        return store.getGroupProperties(group);
    }

    /**
     * Update group's schema validation policy.
     *
     * @param group           Name of group.
     * @param validationRules New validation rules for the group.
     * @return CompletableFuture which is completed when validation policy update completes.
     */
    public CompletableFuture<Void> updateSchemaValidationPolicy(String group, SchemaValidationRules validationRules) {
        return Futures.toVoid(store.getGroupEtag(group)
                                   .thenCompose(etag -> store.updateValidationPolicy(group, etag, validationRules)));
    }

    /**
     * Gets list of event types registered under the group. Event types are identified by {@link SchemaInfo#name}
     *
     * @param group Name of group.
     * @param token Continuation token.
     * @return CompletableFuture which holds list of event types upon completion.
     */
    public CompletableFuture<ListWithToken<String>> getEventTypes(String group, ContinuationToken token) {
        return store.listEventTypes(group, token);
    }

    /**
     * Adds schema to the group. If group is configured with {@link GroupProperties#validateByEventType}, then
     * the {@link SchemaInfo#name} is used to filter previous schemas and apply schema validation policy against schemas
     * of event type.
     * Schema validation rules that are sent to the registry should be a super set of Validation rules set in
     * {@link GroupProperties#schemaValidationRules}
     *
     * @param group  Name of group.
     * @param schema Schema to add.
     * @param rules  Schema validation rules to apply.
     * @return CompletableFuture that holds versionInfo which uniquely identifies where the schema is added in the group.
     */
    public CompletableFuture<VersionInfo> addSchemaIfAbsent(String group, SchemaInfo schema, SchemaValidationRules rules) {
        // 1. get group policy
        // 2. get checker for schema type.
        // validate schema against group policy + rules on schema
        // 3. conditionally update the schema
        CompatibilityChecker checker;

        return store.getGroupEtag(group)
                    .thenCompose(etag -> store.getGroupProperties(group)
                                              .thenCompose(prop -> {
                                                  SchemaValidationRules policy = prop.getSchemaValidationRules();
                                                  if (prop.isValidateByEventType()) {
                                                      String eventTypeName = schema.getName();
                                                      // todo: apply policy
                                                      // get schemas by event type for validation
                                                      return store.addSchemaToGroup(group, etag, schema);
                                                  } else {
                                                      // todo: apply policy
                                                      // get schemas for group for validation
                                                      return store.addSchemaToGroup(group, etag, schema);
                                                  }
                                              }));
    }

    /**
     * Gets schema corresponding to the version.
     *
     * @param group   Name of group.
     * @param version Version which uniquely identifies schema within a group.
     * @return CompletableFuture that holds Schema info corresponding to the version info.
     */
    public CompletableFuture<SchemaInfo> getSchema(String group, VersionInfo version) {
        return store.getSchema(group, version);
    }

    /**
     * Gets encoding info against the requested encoding Id.
     * Encoding Info uniquely identifies a combination of a schemaInfo and compressionType.
     *
     * @param group      Name of group.
     * @param encodingId Encoding id that uniquely identifies a schema within a group.
     * @return CompletableFuture that holds Encoding info corresponding to the encoding id.
     */
    public CompletableFuture<EncodingInfo> getEncodingInfo(String group, EncodingId encodingId) {
        return store.getEncodingInfo(group, encodingId);
    }

    /**
     * Gets an encoding id that uniquely identifies a combination of Schema version and compression type.
     *
     * @param group           Name of group.
     * @param version         version of schema
     * @param compressionType compression type
     * @return CompletableFuture that holds Encoding id for the pair of version and compression type.
     */
    public CompletableFuture<EncodingId> getEncodingId(String group, VersionInfo version, CompressionType compressionType) {
        return store.createOrGetEncodingId(group, version, compressionType);
    }

    /**
     * Gets latest schema and version for the group (or eventType, if specified).
     * For groups configured with {@link GroupProperties#validateByEventType}, the eventType name needs to be supplied to
     * get the latest schema for the event type.
     *
     * @param group    Name of group.
     * @param eventTypeName Event type.
     * @return CompletableFuture that holds Schema with version for the last schema that was added to the group.
     */
    public CompletableFuture<SchemaWithVersion> getLatestSchema(String group, @Nullable String eventTypeName) {
        if (eventTypeName == null) {
            return store.getLatestSchema(group);
        } else {
            return store.getLatestSchema(group, eventTypeName);
        }
    }

    /**
     * Gets all schemas with corresponding versions for the group (or eventTypeName, if specified).
     * For groups configured with {@link GroupProperties#validateByEventType}, the eventTypeName name needs to be supplied to
     * get the latest schema for the eventTypeName. {@link SchemaInfo#name} is used as the eventTypeName name.
     * The order in the list matches the order in which schemas were evolved within the group.
     *
     * @param group    Name of group.
     * @param eventTypeName Event Type.
     * @return CompletableFuture that holds Ordered list of schemas with versions and validation rules for all schemas in the group.
     */
    public CompletableFuture<List<SchemaEvolution>> getGroupEvolutionHistory(String group, @Nullable String eventTypeName) {
        return store.getGroupProperties(group)
                    .thenCompose(prop -> {
                        if (prop.isValidateByEventType()) {
                            if (eventTypeName == null) {
                                throw new InputMismatchException();
                            }
                            return store.getGroupHistoryForEventType(group, eventTypeName);
                        } else {
                            return store.getGroupHistory(group);
                        }
                    });
    }

    /**
     * Gets version corresponding to the schema. If group has been configured with {@link GroupProperties#validateByEventType}
     * the eventType name is taken from the {@link SchemaInfo#name}.
     * For each unique {@link SchemaInfo#schemaData}, there will be a unique monotonically increasing version assigned.
     *
     * @param group  Name of group.
     * @param schema SchemaInfo that captures schema name and schema data.
     * @return CompletableFuture that holds VersionInfo corresponding to schema.
     */
    public CompletableFuture<VersionInfo> getSchemaVersion(String group, SchemaInfo schema) {
        return store.getSchemaVersion(group, schema);
    }

    /**
     * Checks whether given schema is valid by applying validation rules against previous schemas in the group
     * subject to current {@link GroupProperties#schemaValidationRules} policy.
     * If {@link GroupProperties#validateByEventType} is set, the validation is performed against schemas with same 
     * event type identified by {@link SchemaInfo#name}.
     *
     * @param group           Name of group.
     * @param schema          Schema to check for validity.
     * @param validationRules validation rules to apply.
     * @return True if it satisfies validation checks, false otherwise.
     */
    public CompletableFuture<Boolean> validateSchema(String group, SchemaInfo schema, SchemaValidationRules validationRules) {
        // based on compatibility policy, fetch specific schemas for the group and perform validations. 
        // TODO: validate schema

        return store.getGroupProperties(group)
                    .thenApply(prop -> {
                        return true;
                    });
    }
    
    public CompletableFuture<Boolean> canRead(String group, SchemaInfo schema) {
        return store.getGroupProperties(group)
                    .thenApply(prop -> {
                        return true;
                    });
    }
    
    /**
     * Deletes group.  
     * @param group Name of group. 
     * @return CompletableFuture which is completed when group is deleted. 
     */
    public CompletableFuture<Void> deleteGroup(String group) {
        return store.deleteGroup(group);
    }

    /**
     * List of compressions used for encoding in the group. This will be returned only if {@link GroupProperties#enableEncoding}
     * is set to true. 
     *
     * @param group Name of group. 
     * @return CompletableFuture that holds list of compressions used for encoding in the group. 
     */
    public CompletableFuture<List<CompressionType>> getCompressions(String group) {
        return store.getCompressions(group);
    }
}
