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
import io.pravega.schemaregistry.storage.impl.group.records.NamespaceAndGroup;
import lombok.Data;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaFingerprintKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.Key;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaGroupsKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaGroupsList;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaRecord;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaIdKey;
import static io.pravega.schemaregistry.storage.impl.schemas.SchemaRecords.SchemaIdList;

/**
 * In memory groups implementation. 
 */
public class InMemorySchemas implements Schemas<Integer> {
    @GuardedBy("$lock")
    private final Map<Key, Value> schemas = new HashMap<>();

    @Synchronized
    @Override
    public CompletableFuture<Void> addSchema(SchemaInfo schemaInfo, BigInteger fingerprint, Predicate<SchemaInfo> equality, 
                                             String nameSpace, String group) {
        String namespace = nameSpace == null ? "" : nameSpace;
        SchemaFingerprintKey fingerprintKey = new SchemaFingerprintKey(fingerprint);
        Value fingerprintValue = schemas.get(fingerprintKey);
        String schemaId = fingerprintValue == null ? null : findSchemaId(fingerprintValue, equality);

        // add schema and fingerprint
        if (schemaId == null) {
            schemaId = addSchemaAndFingerprint(schemaInfo, fingerprintKey, fingerprintValue);
        }
        // add group reference
        SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
        Value groupsValue = schemas.get(groupsKey);
        NamespaceAndGroup namespaceAndGroup = new NamespaceAndGroup(namespace, group);
        if (groupsValue != null) {
            List<NamespaceAndGroup> list = new ArrayList<>(((SchemaGroupsList) groupsValue.value).getGroupIds());
            list.add(namespaceAndGroup);
            schemas.put(groupsKey, new Value(new SchemaGroupsList(list), groupsValue.version + 1));
        } else {
            schemas.put(groupsKey, new Value(new SchemaGroupsList(Collections.singletonList(namespaceAndGroup)), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Synchronized
    private String addSchemaAndFingerprint(SchemaInfo schemaInfo, SchemaFingerprintKey fingerprintKey, Value fingerprintValue) {
        String schemaId;
        schemaId = UUID.randomUUID().toString();
        SchemaIdKey schemaIdKey = new SchemaIdKey(schemaId);
        schemas.put(schemaIdKey, new Value(new SchemaRecord(schemaInfo), 0));

        if (fingerprintValue == null) {
            schemas.put(fingerprintKey, new Value(new SchemaIdList(Collections.singletonList(schemaId)), 0));
        } else {
            List<String> list = new ArrayList<>(((SchemaIdList) fingerprintValue.getValue()).getSchemaIds());
            list.add(schemaId);
            schemas.put(fingerprintKey, new Value(new SchemaIdList(list), fingerprintValue.version + 1));
        }
        return schemaId;
    }

    @Synchronized
    private String findSchemaId(Value fingerprintValue, Predicate<SchemaInfo> equality) {
        String schemaId;
        SchemaIdList list = (SchemaIdList) fingerprintValue.getValue();
        schemaId = list.getSchemaIds().stream().filter(x -> {
            SchemaIdKey schemaIdKey = new SchemaIdKey(x);
            SchemaInfo schema = ((SchemaRecord) schemas.get(schemaIdKey).getValue()).getSchemaInfo();
            return equality.test(schema);
        }).findAny().orElse(null);
        return schemaId;
    }

    @Synchronized
    @Override
    public CompletableFuture<List<String>> getGroupsUsing(String nameSpace, BigInteger fingerprint, Predicate<SchemaInfo> equality) {
        String namespace = nameSpace == null ? "" : nameSpace;
        SchemaFingerprintKey fingerprintKey = new SchemaFingerprintKey(fingerprint);
        Value fingerprintValue = schemas.get(fingerprintKey);
        String schemaId = fingerprintValue == null ? null : findSchemaId(fingerprintValue, equality);
        if (schemaId == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            SchemaGroupsKey groupsKey = new SchemaGroupsKey(schemaId);
            SchemaGroupsList groupsValue = (SchemaGroupsList) schemas.get(groupsKey).value;

            List<String> groupIds = groupsValue.getGroupIds().stream().filter(x -> x.getNamespace().equals(namespace))
                                               .map(NamespaceAndGroup::getGroupId).collect(Collectors.toList());
            return CompletableFuture.completedFuture(groupIds);
        }
    }

    @Data
    private static class Value {
        private final SchemaRecords.Value value;
        private final int version;
    }

}
