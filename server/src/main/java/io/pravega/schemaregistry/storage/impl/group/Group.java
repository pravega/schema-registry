/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolution;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.contract.exceptions.CodecNotFoundException;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class that implements all storage logic for a group.
 * The group uses a key value store which supports batch updates with optimistic concurrency.
 *
 * @param <V> Type of version used in the index.
 */
@Slf4j
public class Group<V> {
    private static final TableRecords.Etag ETAG = new TableRecords.Etag();
    private static final TableRecords.ValidationPolicyKey VALIDATION_POLICY_KEY = new TableRecords.ValidationPolicyKey();
    private static final TableRecords.GroupPropertyKey GROUP_PROPERTY_KEY = new TableRecords.GroupPropertyKey();
    private static final TableRecords.CodecsKey CODECS_KEY = new TableRecords.CodecsKey();
    private static final TableRecords.ObjectTypesKey OBJECTS_TYPE_KEY = new TableRecords.ObjectTypesKey();
    private static final TableRecords.LatestSchemaVersionKey LATEST_SCHEMA_VERSION_KEY = new TableRecords.LatestSchemaVersionKey();
    private static final TableRecords.LatestEncodingIdKey LATEST_ENCODING_ID_KEY = new TableRecords.LatestEncodingIdKey();
    private static final HashFunction HASH = Hashing.murmur3_128();
    private static final Retry.RetryAndThrowConditionally WRITE_CONFLICT_RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                                      .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);

    private final String groupId;
    private final GroupTable<V> groupTable;
    private final ScheduledExecutorService executor;

    public Group(String groupId, GroupTable<V> groupTable, ScheduledExecutorService executor) {
        this.groupId = groupId;
        this.groupTable = groupTable;
        this.executor = executor;
    }

    public CompletableFuture<Void> create(SchemaType schemaType, Map<String, String> properties, boolean validateByObjectType, SchemaValidationRules schemaValidationRules) {

        return groupTable.addEntry(ETAG, ETAG)
                         .thenCompose(v -> {
                             TableRecords.GroupPropertiesRecord groupProp = new TableRecords.GroupPropertiesRecord(schemaType, validateByObjectType, properties);
                             TableRecords.ValidationRecord validationRecord = new TableRecords.ValidationRecord(schemaValidationRules);
                             CompletableFuture<Void> addProp = Futures.exceptionallyExpecting(groupTable.addEntry(GROUP_PROPERTY_KEY, groupProp),
                                     e -> Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException, null);
                             CompletableFuture<Void> addPolicy = Futures.exceptionallyExpecting(groupTable.addEntry(VALIDATION_POLICY_KEY, validationRecord),
                                     e -> Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException, null);

                             return CompletableFuture.allOf(addProp, addPolicy)
                                                     .whenComplete((r, e) -> {
                                                         if (e == null) {
                                                             log.info("group {} properties created", groupId);
                                                         } else {
                                                             log.error("failed to create group {}", e, groupId);
                                                         }
                                                     });
                         });
    }

    public CompletableFuture<Etag> getCurrentEtag() {
        return groupTable.getEntryWithVersion(ETAG, TableRecords.Etag.class)
                         .thenApply(record -> groupTable.toEtag(record.getVersion()));
    }

    public CompletableFuture<ListWithToken<String>> getObjectTypes() {
        return groupTable.getEntry(OBJECTS_TYPE_KEY, TableRecords.ObjectTypesListValue.class)
                         .thenApply(objectTypes -> {
                             List<String> list = objectTypes == null ? Collections.emptyList() : objectTypes.getObjectTypes();
                             return new ListWithToken<>(list, null);
                         });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas() {
        return getSchemas(0);
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(int fromPos) {
        return groupTable.getEntry(LATEST_SCHEMA_VERSION_KEY, TableRecords.LatestSchemaVersionValue.class)
                         .thenCompose(latest -> {
                             int endPos = latest == null ? 0 : latest.getVersion().getOrdinal() + 1;
                             List<TableRecords.VersionKey> keys = IntStream
                                     .range(fromPos, endPos)
                                     .boxed()
                                     .map(TableRecords.VersionKey::new)
                                     .collect(Collectors.toList());
                             return groupTable.getEntries(keys, TableRecords.SchemaRecord.class);
                         }).thenApply(entries -> {
                    List<SchemaWithVersion> list = entries.stream().map(x -> new SchemaWithVersion(x.getSchemaInfo(), x.getVersionInfo()))
                                                          .collect(Collectors.toList());
                    return new ListWithToken<>(list, null);
                });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName) {
        return getSchemas(objectTypeName, 0);
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String objectTypeName, int fromPos) {
        return getSchemas(fromPos)
                .thenApply(schemas -> {
                    List<SchemaWithVersion> list = schemas
                            .getList().stream().filter(x -> x.getSchema().getName().equals(objectTypeName)).collect(Collectors.toList());
                    return new ListWithToken<>(list, null);
                });
    }

    public CompletableFuture<SchemaInfo> getSchema(int versionOrdinal) {
        return groupTable.getEntry(new TableRecords.VersionKey(versionOrdinal), TableRecords.SchemaRecord.class)
                         .thenApply(TableRecords.SchemaRecord::getSchemaInfo);
    }

    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo) {
        long fingerprint = getFingerprint(schemaInfo);
        TableRecords.SchemaInfoKey key = new TableRecords.SchemaInfoKey(fingerprint);

        return groupTable.getEntry(key, TableRecords.SchemaVersionValue.class)
                         .thenCompose(record -> {
                             if (record != null) {
                                 return findVersion(record.getVersions(), schemaInfo);
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         })
                         .thenApply(version -> {
                             if (version == null) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("Schema=%s", fingerprint));
                             } else {
                                 return version;
                             }
                         });
    }

    public CompletableFuture<EncodingId> createEncodingId(VersionInfo versionInfo, CodecType codecType, Etag etag) {
        return generateNewEncodingId(versionInfo, codecType, etag);
    }

    public CompletableFuture<EncodingInfo> getEncodingInfo(EncodingId encodingId) {
        TableRecords.EncodingIdRecord encodingIdIndex = new TableRecords.EncodingIdRecord(encodingId);
        return groupTable.getEntry(encodingIdIndex, TableRecords.EncodingInfoRecord.class)
                         .thenCompose(encodingInfo -> getSchema(encodingInfo.getVersionInfo().getOrdinal())
                                 .thenApply(schemaInfo -> new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, encodingInfo.getCodecType())));
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema() {
        return groupTable.getEntry(LATEST_SCHEMA_VERSION_KEY, TableRecords.LatestSchemaVersionValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 return rec.getVersion();
                             }
                         })
                         .thenCompose(versionInfo -> {
                             if (versionInfo != null) {
                                 return getSchema(versionInfo.getOrdinal()).thenApply(schema -> new SchemaWithVersion(schema, versionInfo));
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema(String objectTypeName) {
        TableRecords.LatestSchemaVersionForObjectTypeKey key = new TableRecords.LatestSchemaVersionForObjectTypeKey(objectTypeName);
        return groupTable.getEntry(key, TableRecords.LatestSchemaVersionValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 return rec.getVersion();
                             }
                         })
                         .thenCompose(versionInfo -> {
                             if (versionInfo != null) {
                                 return getSchema(versionInfo.getOrdinal()).thenApply(schema -> new SchemaWithVersion(schema, versionInfo));
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }

    public CompletableFuture<List<CodecType>> getCodecTypes() {
        return groupTable.getEntry(CODECS_KEY, TableRecords.CodecsListValue.class)
                         .thenApply(codecs -> {
                             if (codecs == null) {
                                 return Collections.singletonList(CodecType.None);
                             } else {
                                 return codecs.getCodecs();
                             }
                         });
    }

    public CompletableFuture<Void> addCodec(CodecType codecType) {
        // get all codecs. if codec doesnt exist, add it to log. let it get synced to the table. 
        // generate encoding id will only generate if the codec is already registered.
        return WRITE_CONFLICT_RETRY.runAsync(() -> getCurrentEtag()
                .thenCompose(etag -> groupTable.getEntryWithVersion(CODECS_KEY, TableRecords.CodecsListValue.class)
                                               .thenCompose(rec -> addCodec(codecType, etag, rec))), executor);
    }

    private CompletionStage<Void> addCodec(CodecType codecType, Etag etag, GroupTable.Value<TableRecords.CodecsListValue, V> rec) {
        if (rec.getValue() == null || !rec.getValue().getCodecs().contains(codecType)) {
            List<Map.Entry<TableRecords.TableKey, GroupTable.Value<TableRecords.TableValue, V>>> entries = new ArrayList<>();
            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));
            V version = rec.getVersion();
            List<CodecType> codecs;
            if (rec.getValue() == null) {
                codecs = Collections.singletonList(codecType);
            } else {
                codecs = new LinkedList<>(rec.getValue().getCodecs());
                codecs.add(codecType);
            }
            TableRecords.CodecsListValue updated = new TableRecords.CodecsListValue(codecs);
            entries.add(new AbstractMap.SimpleEntry<>(CODECS_KEY, new GroupTable.Value<>(updated, version)));

            return groupTable.updateEntries(entries);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<ListWithToken<SchemaEvolution>> getHistory() {
        return groupTable.getEntry(LATEST_SCHEMA_VERSION_KEY, TableRecords.LatestSchemaVersionValue.class)
                         .thenCompose(latestPos -> {
                             List<TableRecords.VersionKey> keys = IntStream.range(0, latestPos.getVersion().getOrdinal() + 1)
                                                                           .boxed().map(TableRecords.VersionKey::new)
                                                                           .collect(Collectors.toList());
                             return groupTable.getEntries(keys, TableRecords.SchemaRecord.class);
                         }).thenApply(entries -> {
                    List<SchemaEvolution> list = entries
                            .stream().map(x -> new SchemaEvolution(x.getSchemaInfo(), x.getVersionInfo(), x.getValidationRules()))
                            .collect(Collectors.toList());
                    return new ListWithToken<>(list, null);
                });
    }

    public CompletableFuture<ListWithToken<SchemaEvolution>> getHistory(String objectTypeName) {
        return getHistory().thenApply(list ->
                new ListWithToken<>(list.getList().stream().filter(x -> x.getSchema().getName().equals(objectTypeName))
                                        .collect(Collectors.toList()), null));
    }

    public CompletableFuture<VersionInfo> addSchemaToGroup(SchemaInfo schemaInfo, GroupProperties prop, Etag etag) {
        List<TableRecords.TableKey> keys = new ArrayList<>();
        keys.add(LATEST_SCHEMA_VERSION_KEY);
        TableRecords.SchemaInfoKey schemaInfoKey = new TableRecords.SchemaInfoKey(getFingerprint(schemaInfo));
        keys.add(schemaInfoKey);

        if (prop.isValidateByObjectType()) {
            keys.add(new TableRecords.LatestSchemaVersionForObjectTypeKey(schemaInfo.getName()));
            keys.add(OBJECTS_TYPE_KEY);
        }

        return groupTable.getEntriesWithVersion(keys, TableRecords.TableValue.class).thenCompose(values -> {
            TableRecords.LatestSchemaVersionValue latest = (TableRecords.LatestSchemaVersionValue) values.get(0).getValue();
            V latestVersion = values.get(0).getVersion();
            TableRecords.SchemaVersionValue schemaIndex = (TableRecords.SchemaVersionValue) values.get(1).getValue();
            V schemaIndexVersion = values.get(1).getVersion();
            int nextOrdinal = latest == null ? 0 : latest.getVersion().getOrdinal() + 1;
            int nextVersion;

            if (prop.isValidateByObjectType()) {
                TableRecords.LatestSchemaVersionValue objectLatestVersion = (TableRecords.LatestSchemaVersionValue) values.get(2).getValue();
                nextVersion = objectLatestVersion == null ? 0 : objectLatestVersion.getVersion().getVersion() + 1;
            } else {
                nextVersion = nextOrdinal;
            }
            VersionInfo next = new VersionInfo(schemaInfo.getName(), nextVersion, nextOrdinal);

            List<Map.Entry<TableRecords.TableKey, GroupTable.Value<TableRecords.TableValue, V>>> entries = new LinkedList<>();
            // 0. etag
            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

            // 1. version info key. add
            entries.add(new AbstractMap.SimpleEntry<>(new TableRecords.VersionKey(next.getOrdinal()),
                    new GroupTable.Value<>(new TableRecords.SchemaRecord(schemaInfo, next, prop.getSchemaValidationRules()), null)));

            // 2. schema info key. update
            List<VersionInfo> versions;
            if (schemaIndex == null) {
                versions = Collections.singletonList(next);
            } else {
                versions = new ArrayList<>(schemaIndex.getVersions());
                versions.add(next);
            }
            entries.add(new AbstractMap.SimpleEntry<>(schemaInfoKey,
                    new GroupTable.Value<>(new TableRecords.SchemaVersionValue(versions), schemaIndexVersion)));

            // 3. latest schema version
            entries.add(new AbstractMap.SimpleEntry<>(LATEST_SCHEMA_VERSION_KEY,
                    new GroupTable.Value<>(new TableRecords.LatestSchemaVersionValue(next), latestVersion)));

            if (prop.isValidateByObjectType()) {
                // 3.1 latest for object type
                V objectLatestVersionVersion = values.get(2).getVersion();
                entries.add(new AbstractMap.SimpleEntry<>(new TableRecords.LatestSchemaVersionForObjectTypeKey(
                        schemaInfo.getName()),
                        new GroupTable.Value<>(new TableRecords.LatestSchemaVersionValue(next), objectLatestVersionVersion)));
            }

            // 4. object types list
            if (prop.isValidateByObjectType()) {
                TableRecords.ObjectTypesListValue objectTypes = (TableRecords.ObjectTypesListValue) values.get(3).getValue();
                V objectTypesVersion = values.get(3).getVersion();

                List<String> list = objectTypes == null ? new ArrayList<>() :
                        Lists.newArrayList(objectTypes.getObjectTypes());
                if (!list.contains(schemaInfo.getName())) {
                    list.add(schemaInfo.getName());
                }
                entries.add(new AbstractMap.SimpleEntry<>(OBJECTS_TYPE_KEY,
                        new GroupTable.Value<>(
                                new TableRecords.ObjectTypesListValue(list), objectTypesVersion)));
            }
            return groupTable.updateEntries(entries).thenApply(v -> next);
        });
    }

    public CompletableFuture<Void> updateValidationPolicy(SchemaValidationRules policy, Etag etag) {
        return groupTable.getEntryWithVersion(VALIDATION_POLICY_KEY, TableRecords.ValidationRecord.class)
                         .thenCompose(entry -> {
                             if (entry.getValue().getValidationRules().equals(policy)) {
                                 return CompletableFuture.completedFuture(null);
                             } else {
                                 List<Map.Entry<TableRecords.TableKey, GroupTable.Value<TableRecords.TableValue, V>>> entries = new ArrayList<>();
                                 entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

                                 TableRecords.ValidationRecord updated = new TableRecords.ValidationRecord(policy);
                                 entries.add(new AbstractMap.SimpleEntry<>(VALIDATION_POLICY_KEY, new GroupTable.Value<>(updated, entry.getVersion())));

                                 return groupTable.updateEntries(entries);
                             }
                         });
    }

    public CompletableFuture<GroupProperties> getGroupProperties() {
        List<? extends TableRecords.TableKey> keys = Lists.newArrayList(GROUP_PROPERTY_KEY, VALIDATION_POLICY_KEY);
        return groupTable.getEntries(keys, TableRecords.TableValue.class)
                         .thenApply(entries -> {
                             TableRecords.GroupPropertiesRecord properties = (TableRecords.GroupPropertiesRecord) entries.get(0);
                             TableRecords.ValidationRecord validationRecord = (TableRecords.ValidationRecord) entries.get(1);
                             return new GroupProperties(properties.getSchemaType(), validationRecord.getValidationRules(),
                                     properties.isValidateByObjectType(),
                                     properties.getProperties());
                         });
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HASH.hashBytes(schemaInfo.getSchemaData()).asLong();
    }

    private CompletableFuture<EncodingId> generateNewEncodingId(VersionInfo versionInfo, CodecType codecType, Etag etag) {
        return getCodecTypes()
                .thenCompose(codecs -> {
                    if (codecs.contains(codecType)) {
                        TableRecords.LatestEncodingIdKey key = new TableRecords.LatestEncodingIdKey();
                        return groupTable.getEntryWithVersion(key, TableRecords.LatestEncodingIdValue.class).thenCompose(current -> {
                            EncodingId nextEncodingId = current.getValue() == null ? new EncodingId(0) :
                                    new EncodingId(current.getValue().getEncodingId().getId() + 1);                                    
                            V encodingIdVersion = current.getVersion();

                            List<Map.Entry<TableRecords.TableKey, GroupTable.Value<TableRecords.TableValue, V>>> entries = new LinkedList<>();

                            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

                            TableRecords.EncodingIdRecord idIndex = new TableRecords.EncodingIdRecord(nextEncodingId);
                            TableRecords.EncodingInfoRecord infoIndex = new TableRecords.EncodingInfoRecord(versionInfo, codecType);
                            // add new entries for encoding id and info
                            entries.add(new AbstractMap.SimpleEntry<>(idIndex, new GroupTable.Value<>(infoIndex, null)));
                            entries.add(new AbstractMap.SimpleEntry<>(infoIndex, new GroupTable.Value<>(idIndex, null)));
                            // update
                            entries.add(new AbstractMap.SimpleEntry<>(LATEST_ENCODING_ID_KEY,
                                    new GroupTable.Value<>(new TableRecords.LatestEncodingIdValue(nextEncodingId), encodingIdVersion)));
                            return groupTable.updateEntries(entries)
                                             .thenApply(v -> nextEncodingId);
                        });
                    } else {
                        throw new CodecNotFoundException(String.format("codec %s not registered", codecType));
                    }
                });
    }

    public CompletableFuture<Either<EncodingId, Etag>> getEncodingId(VersionInfo versionInfo, CodecType codecType) {
        TableRecords.EncodingInfoRecord encodingInfoIndex = new TableRecords.EncodingInfoRecord(versionInfo, codecType);
        return groupTable.getEntry(encodingInfoIndex, TableRecords.EncodingIdRecord.class)
                         .thenCompose(record -> {
                             if (record == null) {
                                 return getCurrentEtag().thenApply(Either::right);
                             } else {
                                 return CompletableFuture.completedFuture(Either.left(record.getEncodingId()));
                             }
                         });
    }

    private CompletableFuture<VersionInfo> findVersion(List<VersionInfo> versions, SchemaInfo toFind) {
        AtomicReference<VersionInfo> found = new AtomicReference<>();
        Iterator<VersionInfo> iterator = versions.iterator();
        return Futures.loop(() -> {
            return iterator.hasNext() && found.get() == null;
        }, () -> {
            VersionInfo version = iterator.next();
            return getSchema(version.getOrdinal())
                    .thenAccept(schema -> {
                        if (Arrays.equals(schema.getSchemaData(), toFind.getSchemaData()) && schema.getName().equals(toFind.getName())) {
                            found.set(version);
                        }
                    });
        }, executor).thenApply(v -> found.get());
    }
}