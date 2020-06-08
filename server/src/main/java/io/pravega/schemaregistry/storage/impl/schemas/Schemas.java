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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Global schemas table to store all schemas and references to groups using those schemas. 
 * 
 * @param <T> Type of Version. 
 */
public interface Schemas<T> {
    CompletableFuture<Void> addNewSchema(SchemaInfo schemaInfo, String namespace, String group);

    CompletableFuture<List<String>> getGroupsUsing(String namespace, SchemaInfo schemaInfo);

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
