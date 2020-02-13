/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

import io.pravega.schemaregistry.contract.SchemaRegistryContract;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public interface SchemaStore {
    CompletableFuture<Void> createScope(String scope);
    
    CompletableFuture<Void> createGroupInScope(String scope, String group, SchemaRegistryContract.GroupProperties groupProperties);
    
    CompletableFuture<ContinuationToken> listGroups(String scope, String group, SchemaRegistryContract.GroupProperties groupProperties, 
                                                    @Nullable ContinuationToken token);
    
    CompletableFuture<ContinuationToken> listSubGroups(String scope, String group, SchemaRegistryContract.GroupProperties groupProperties,
                                                    @Nullable ContinuationToken token);

    CompletableFuture<SchemaRegistryContract.VersionInfo> conditionallyAddSchemaToGroup(String scope, String group,
                                                                                   SchemaRegistryContract.SchemaInfo schemaInfo, 
                                                                                   Etag etag);

    CompletableFuture<SchemaRegistryContract.VersionInfo> conditionallyAddSchemaToSubgroup(String scope, String group, String subgroup, 
                                                                                      SchemaRegistryContract.SchemaInfo schemaInfo,
                                                                                      Etag etag);

    
}
