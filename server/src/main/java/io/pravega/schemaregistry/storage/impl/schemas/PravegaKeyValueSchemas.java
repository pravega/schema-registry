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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.common.HashUtil;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.client.TableStore;
import io.pravega.schemaregistry.storage.client.Version;
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaFingerprintKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaGroupsKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaGroupsList;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaRecord;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaIdKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaIdList;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.KeySerializer;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.fromBytes;

public class PravegaKeyValueSchemas implements Schemas<Version> {
    private static final String SCHEMAS = TableStore.SCHEMA_REGISTRY_SCOPE + "/schemas/0";
    private static final KeySerializer KEY_SERIALIZER = new KeySerializer();

    private final TableStore tableStore;

    public PravegaKeyValueSchemas(TableStore tableStore) {
        this.tableStore = tableStore;
    }

    @Override
    public CompletableFuture<Void> addSchema(SchemaInfo schemaInfo, String nameSpace, String group) {
        String namespace = nameSpace == null ? "" : nameSpace;
        // 1. check if schema exists -- get fingerprint -- get all schemas in the fingerprint list.. 
        // 2. if it doesnt exist, generate a new id and add it to id and fingerprint list and add schema id entry atomically. 
        // (this can fail with write conflict if multiple concurrent attempts are made. keep retrying). 
        // 3. add the group name to the schema id groups list. get and set.  
        SchemaFingerprintKey fingerprintKey = new
                SchemaFingerprintKey(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()));
        return withCreateSchemasTableIfAbsent(() -> Futures.exceptionallyExpecting(tableStore.getEntry(SCHEMAS,
                KEY_SERIALIZER.toBytes(fingerprintKey),
                x -> fromBytes(SchemaFingerprintKey.class, x, SchemaIdList.class)),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                null)
                      .thenCompose(fingerprintEntry -> {
                          return findSchemaId(schemaInfo, fingerprintEntry)
                                  .thenCompose(schemaId -> {
                                      if (schemaId == null) {
                                          return addNewSchemaRecord(schemaInfo, fingerprintKey, fingerprintEntry);
                                      } else {
                                          return CompletableFuture.completedFuture(schemaId);
                                      }
                                  })
                                  .thenCompose(schemaId -> addGroupReferenceForSchema(namespace, group, schemaId));
                      }));
    }

    private CompletionStage<Void> addGroupReferenceForSchema(String namespace, String group, String schemaId) {
        SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
        return Futures.exceptionallyExpecting(tableStore.getEntry(SCHEMAS, KEY_SERIALIZER.toBytes(groupsKey),
                x -> fromBytes(SchemaGroupsKey.class, x, SchemaGroupsList.class)),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null)
                      .thenCompose(groups -> {
                          Version groupsVersion = groups == null ? null : groups.getVersion();
                          List<NamespaceAndGroup> groupsList = groups == null ? new ArrayList<>() :
                                  new ArrayList<>(groups.getRecord().getGroupIds());
                          NamespaceAndGroup namespaceAndGroup = new NamespaceAndGroup(namespace, group);
                          if (!groupsList.contains(namespaceAndGroup)) {
                              groupsList.add(namespaceAndGroup);
                              return Futures.toVoid(tableStore.updateEntry(SCHEMAS,
                                      KEY_SERIALIZER.toBytes(groupsKey),
                                      new SchemaGroupsList(groupsList).toBytes(), groupsVersion));
                          } else {
                              return CompletableFuture.completedFuture(null);
                          }
                      });
    }

    private CompletionStage<String> addNewSchemaRecord(SchemaInfo schemaInfo, SchemaFingerprintKey fingerprintKey,
                                                       Version.VersionedRecord<SchemaIdList> fingerprintEntry) {
        String id;
        Map<byte[], Map.Entry<byte[], Version>> entries = new HashMap<>();
        Version fingerprintKeyVersion = fingerprintEntry == null ? null : fingerprintEntry.getVersion();
        List<String> schemaIdList = fingerprintEntry == null ? new ArrayList<>() :
                new ArrayList<>(fingerprintEntry.getRecord().getSchemaIds());

        id = UUID.randomUUID().toString();
        // schema not present. add schema and update fingerprint record
        // 1. fingerprint 
        schemaIdList.add(id);
        entries.put(KEY_SERIALIZER.toBytes(fingerprintKey),
                new AbstractMap.SimpleEntry<>(new SchemaIdList(schemaIdList).toBytes(), fingerprintKeyVersion));
        // 2. add schemaId record
        SchemaIdKey key = new SchemaIdKey(id);
        entries.put(KEY_SERIALIZER.toBytes(key),
                new AbstractMap.SimpleEntry<>(new SchemaRecord(schemaInfo).toBytes(), null));
        return tableStore.updateEntries(SCHEMAS, entries)
                         .thenApply(v -> id);
    }

    private CompletableFuture<String> findSchemaId(SchemaInfo schemaInfo, Version.VersionedRecord<SchemaIdList> fingerprintEntry) {
        CompletableFuture<String> future;
        if (fingerprintEntry != null) {
            // findSchema
            future = Futures.allOfWithResults(fingerprintEntry
                    .getRecord().getSchemaIds().stream()
                    .collect(Collectors.toMap(x -> x, x -> {
                        SchemaIdKey schemaIdKey = new SchemaIdKey(x);
                        Version.VersionedRecord<SchemaRecord> cachedValue = tableStore.getCachedRecord(SCHEMAS, schemaIdKey, SchemaRecord.class);
                        if (cachedValue != null) {
                            return CompletableFuture.completedFuture(cachedValue);
                        } else {
                            return tableStore.getEntry(SCHEMAS, KEY_SERIALIZER.toBytes(schemaIdKey),
                                    y -> fromBytes(SchemaIdKey.class, y, SchemaRecord.class))
                                             .thenApply(entry -> {
                                                 tableStore.cacheRecord(SCHEMAS, schemaIdKey, entry);
                                                 return entry;
                                             });
                        }
                    }))).thenApply(schemas -> schemas.entrySet().stream().filter(x -> {
                SchemaInfo schema = x.getValue().getRecord().getSchemaInfo();
                return schema.getType().equals(schemaInfo.getType())
                        && schema.getSerializationFormat().equals(schemaInfo.getSerializationFormat())
                        && Arrays.equals(schema.getSchemaData().array(), schemaInfo.getSchemaData().array());
            }).map(Map.Entry::getKey).findAny().orElse(null));
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        return future;
    }

    @Override
    public CompletableFuture<List<String>> getGroupsUsing(String nameSpace, SchemaInfo schemaInfo) {
        String namespace = nameSpace == null ? "" : nameSpace;
        SchemaFingerprintKey fingerprintKey = new
                SchemaFingerprintKey(HashUtil.getFingerprint(schemaInfo.getSchemaData().array()));
        return withCreateSchemasTableIfAbsent(() -> Futures.exceptionallyExpecting(tableStore.getEntry(SCHEMAS,
                KEY_SERIALIZER.toBytes(fingerprintKey),
                x -> fromBytes(SchemaFingerprintKey.class, x, SchemaIdList.class)),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException,
                null)
                      .thenCompose(fingerprintEntry -> findSchemaId(schemaInfo, fingerprintEntry))
                      .thenCompose(schemaId -> {
                          if (schemaId == null) {
                              return CompletableFuture.completedFuture(Collections.emptyList());
                          } else {
                              SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
                              return Futures.exceptionallyExpecting(tableStore.getEntry(SCHEMAS, KEY_SERIALIZER.toBytes(groupsKey),
                                      x -> fromBytes(SchemaGroupsKey.class, x, SchemaGroupsList.class)),
                                      e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null)
                                            .thenApply(groups -> {
                                                if (groups == null) {
                                                    return Collections.emptyList();
                                                } else {
                                                    return groups.getRecord().getGroupIds()
                                                            .stream().filter(x -> x.getNamespace().equals(namespace))
                                                                 .map(NamespaceAndGroup::getGroupId).collect(Collectors.toList());
                                                }
                                            });
                          }
                      }));
    }

    private <T> CompletableFuture<T> withCreateSchemasTableIfAbsent(Supplier<CompletableFuture<T>> supplier) {
        return Futures.exceptionallyComposeExpecting(supplier.get(),
                e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataContainerNotFoundException,
                () -> tableStore.createTable(SCHEMAS).thenCompose(v -> supplier.get()));
    }

}
