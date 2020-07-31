/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.schemas;

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import lombok.Data;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Global schemas table to store all schemas and references to groups using those schemas. 
 * 
 * @param <T> Type of Version. 
 */
public interface Schemas<T> {
    /**
     * Add new schema to the global pool of schemas. Also add a reference that the schema is being attempted to be
     * added to the group. 
     * If the schema was previously added then the group's reference is updated. It is an idempotent operation. 
     * Note that adding a group reference does not guarantee that the schema will be added to the group. 
     * This merely indicates that the said schema was attempted to be added to the group and the truth about whether 
     * the schema got added to the group or not should be found in the group metadata. 
     * 
     * @param schemaInfo schema to add. 
     * @param namespace namespace for the group.
     * @param group group name. 
     * @param fingerprint sha 256 hash of normalized schema binary
     * @param equality checks if a schema info object is equal to the supplied schemaInfo. 
     * @return CompletableFuture which completes when schema and corresponding group reference is added to the global 
     * schemas metadata.
     */
    CompletableFuture<Void> addSchema(SchemaInfo schemaInfo, BigInteger fingerprint, Predicate<SchemaInfo> equality, 
                                      String namespace, String group);

    /**
     * Returns names of groups in the given namespace where the schema was attempted to be added. This returns groups where 
     * schema addition was attempted. If the schema addition to the group had failed, or if the schema was deleted from the group
     * then this schema may not be found in the group. 
     * The reference is merely suggestive and not absolute truth about the reference. 
     * 
     * @param namespace namespace
     * @param fingerprint sha 256 hash of schema's normalized binary
     * @param equality checks if a schema info object is equal to the schema whose fingerprint is supplied. 
     * @return CompletableFuture which when completed will hold a list of group names in the namespace where the schema
     * may have been added. 
     */
    CompletableFuture<List<String>> getGroupsUsing(String namespace, BigInteger fingerprint, Predicate<SchemaInfo> equality);

    @Data
    class Value<T extends TableRecords.TableValue, V> {
        private final T value;
        private final V version;
    }

    @Data
    class Entry {
        private final TableRecords.TableKey key;
        private final TableRecords.TableValue value;
    }
}
