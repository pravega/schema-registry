/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.util.JsonFormat;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.common.HashUtil;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.*;

/**
 * Class that implements all storage logic for a group.
 * The group uses a key value store which supports batch updates with optimistic concurrency.
 *
 * @param <V> Type of version used in the index.
 */
@Slf4j
public class Group<V> {
    private static final TableRecords.Etag ETAG = new TableRecords.Etag();
    private static final ValidationPolicyKey VALIDATION_POLICY_KEY = new ValidationPolicyKey();
    private static final GroupPropertyKey GROUP_PROPERTY_KEY = new GroupPropertyKey();
    private static final CodecTypesKey CODECS_TYPE_KEY = new CodecTypesKey();
    private static final SchemaTypesKey SCHEMA_TYPES_KEY = new SchemaTypesKey();
    private static final LatestSchemaVersionKey LATEST_SCHEMA_VERSION_KEY = new LatestSchemaVersionKey();
    private static final LatestEncodingIdKey LATEST_ENCODING_ID_KEY = new LatestEncodingIdKey();
    private static final Retry.RetryAndThrowConditionally WRITE_CONFLICT_RETRY = Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                                                                                      .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);

    private final GroupTable<V> groupTable;
    private final ScheduledExecutorService executor;

    public Group(GroupTable<V> groupTable, ScheduledExecutorService executor) {
        this.groupTable = groupTable;
        this.executor = executor;
    }

    public CompletableFuture<Boolean> create(SerializationFormat serializationFormat, Map<String, String> properties,
                                          boolean allowMultipleTypes, SchemaValidationRules schemaValidationRules) {
        List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new ArrayList<>();
        entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, null)));

        GroupPropertiesRecord groupProp = new GroupPropertiesRecord(serializationFormat, allowMultipleTypes, properties);
        entries.add(new AbstractMap.SimpleEntry<>(GROUP_PROPERTY_KEY, new GroupTable.Value<>(groupProp, null)));

        ValidationRecord validationRecord = new ValidationRecord(schemaValidationRules);
        entries.add(new AbstractMap.SimpleEntry<>(VALIDATION_POLICY_KEY, new GroupTable.Value<>(validationRecord, null)));

        return Futures.exceptionallyComposeExpecting(groupTable.updateEntries(entries).thenApply(x -> true), 
                      e -> Exceptions.unwrap(e) instanceof StoreExceptions.WriteConflictException, 
                () -> compareWithExisting(groupProp, validationRecord))
                      .handle((r, e) -> {
                          if (e == null) {
                              return r;
                          } else {
                              throw new CompletionException(e);
                          }
                      });
    }

    private CompletableFuture<Boolean> compareWithExisting(GroupPropertiesRecord groupProp, 
                                                           ValidationRecord validationRecord) {
        List<TableKey> keys = Lists.newArrayList(GROUP_PROPERTY_KEY, VALIDATION_POLICY_KEY);
        return groupTable.getEntries(keys, TableValue.class)
                .thenApply(entries -> {
                    GroupPropertiesRecord prop = (GroupPropertiesRecord) entries.get(0);
                    ValidationRecord validation = (ValidationRecord) entries.get(1);
                    return prop.equals(groupProp) && validationRecord.equals(validation);
                });
    }

    public CompletableFuture<Etag> getCurrentEtag() {
        return groupTable.getEntryWithVersion(ETAG, TableRecords.Etag.class)
                         .thenApply(record -> groupTable.toEtag(record.getVersion()));
    }

    public CompletableFuture<List<SchemaWithVersion>> getLatestSchemas() {
        return groupTable.getEntry(SCHEMA_TYPES_KEY, SchemaTypesListValue.class)
                         .thenCompose(types -> {
                             List<String> schemas = types == null ? Collections.emptyList() : types.getTypes();
                             return Futures.allOfWithResults(schemas.stream().map(this::getLatestSchemaVersion).collect(Collectors.toList()))
                                     .thenApply(latestSchemasList -> latestSchemasList.stream().filter(Objects::nonNull).collect(Collectors.toList()));
                         });
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas() {
        return getSchemas(0);
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas(int fromPos) {
        return getSchemaRecords(fromPos).thenApply(entries -> entries.stream().map(x -> new SchemaWithVersion(x.getSchemaInfo(), x.getVersionInfo()))
                                                        .collect(Collectors.toList()));
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas(String type) {
        return getSchemas(type, 0);
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas(String type, int fromPos) {
        return getSchemas(fromPos)
                .thenApply(schemas -> schemas.stream().filter(x -> x.getSchemaInfo().getType().equals(type))
                                             .collect(Collectors.toList()));
    }

    private CompletableFuture<List<SchemaRecord>> getSchemaRecords(int fromPos) {
        return groupTable.getEntry(LATEST_SCHEMA_VERSION_KEY, LatestSchemaVersionValue.class)
                         .thenCompose(latestPos -> {
                             int endPos = latestPos == null ? 0 : latestPos.getVersion().getOrdinal() + 1;

                             List<TableKey> keys = new ArrayList<>();
                             List<VersionKey> versionKeys = IntStream.range(fromPos, endPos)
                                                                                  .boxed().map(VersionKey::new)
                                                                                  .collect(Collectors.toList());
                             List<VersionDeletedRecord> deletedKeys = IntStream.range(fromPos, endPos)
                                                                                            .boxed().map(VersionDeletedRecord::new)
                                                                                            .collect(Collectors.toList());
                             keys.addAll(versionKeys);
                             keys.addAll(deletedKeys);
                             return groupTable.getEntriesWithVersion(keys, TableValue.class);
                         }).thenApply(entries -> {
                    List<SchemaRecord> schemaRecords = new ArrayList<>();
                    // Note: the order in which we add keys is - first add version keys followed by deleted keys. 
                    // so all schema records would be present before deleted records. And most of the deleted records 
                    // will be null 

                    for (GroupTable.Value<TableValue, V> entry : entries) {
                        if (entry.getValue() instanceof SchemaRecord) {
                            schemaRecords.add((SchemaRecord) entry.getValue());
                        }
                        if (entry.getValue() instanceof VersionDeletedRecord) {
                            schemaRecords.removeIf(x -> x.getVersionInfo().getOrdinal() ==
                                    ((VersionDeletedRecord) entry.getValue()).getOrdinal());
                        }
                    }

                    return schemaRecords;
                });
    }

    private CompletableFuture<Integer> getVersionOrdinal(String schemaType, int version) {
        return groupTable.getEntry(new SchemaTypeVersionKey(schemaType, version), VersionOrdinalValue.class)
                .thenApply(x -> {
                    if (x != null) {
                        return x.getOrdinal();
                    } else {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("version not found %s %s", schemaType, version));
                    }
                });
    }

    public CompletableFuture<Void> deleteSchema(String schemaType, int version, Etag etag) {
        return getVersionOrdinal(schemaType, version)
                .thenCompose(versionOrdinal -> deleteSchema(versionOrdinal, etag));
    }
    
    public CompletableFuture<Void> deleteSchema(int versionOrdinal, Etag etag) {
        VersionDeletedRecord versionDeletedRecord = new VersionDeletedRecord(versionOrdinal);
        return groupTable.getEntry(versionDeletedRecord, VersionDeletedRecord.class)
                .thenCompose(entry -> {
                    if (entry == null) {
                        List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new ArrayList<>();
                        entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

                        entries.add(new AbstractMap.SimpleEntry<>(versionDeletedRecord,
                                new GroupTable.Value<>(versionDeletedRecord, null)));
                        return groupTable.updateEntries(entries);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<SchemaInfo> getSchema(String schemaType, int version) {
        return getVersionOrdinal(schemaType, version)
                .thenCompose(versionOrdinal -> getSchema(versionOrdinal, false));
    }

    public CompletableFuture<SchemaInfo> getSchema(int versionOrdinal) {
        return getSchema(versionOrdinal, false);
    }

    private CompletableFuture<SchemaInfo> getSchema(int versionOrdinal, boolean throwOnDeleted) {
        List<? extends TableKey> keys = Lists.newArrayList(new VersionKey(versionOrdinal), 
                new VersionDeletedRecord(versionOrdinal));
        return groupTable.getEntriesWithVersion(keys, TableValue.class)
                         .thenApply(entries -> {
                             if (entries.get(1).getValue() != null && throwOnDeleted) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, Integer.toString(versionOrdinal));
                             } else {
                                 TableValue value = entries.get(0).getValue();
                                 if (value != null) {
                                     return ((SchemaRecord) value).getSchemaInfo();
                                 } else {
                                     throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "schema version not found");
                                 }
                             }
                         });
    }

    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo) {
        long fingerprint = getFingerprint(schemaInfo);
        SchemaFingerprintKey key = new SchemaFingerprintKey(fingerprint);

        return groupTable.getEntry(key, SchemaVersionList.class)
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

    public CompletableFuture<EncodingId> createEncodingId(VersionInfo versionInfo, String codecType, Etag etag) {
        return generateNewEncodingId(versionInfo, codecType, etag);
    }

    public CompletableFuture<EncodingInfo> getEncodingInfo(EncodingId encodingId) {
        EncodingIdRecord encodingIdIndex = new EncodingIdRecord(encodingId);
        return groupTable.getEntry(encodingIdIndex, EncodingInfoRecord.class)
                         .thenCompose(encodingInfo -> {
                             if (encodingInfo == null) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, 
                                         String.format("encoding id not found %s", encodingId.getId()));
                             }
                             return getSchema(encodingInfo.getVersionInfo().getOrdinal())
                                     .thenApply(schemaInfo -> new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, encodingInfo.getCodecType()));
                         });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion() {
        return groupTable.getEntry(LATEST_SCHEMA_VERSION_KEY, LatestSchemaVersionValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 return rec.getVersion();
                             }
                         })
                         .thenCompose(versionInfo -> {
                             if (versionInfo != null) {
                                 return Futures.exceptionallyComposeExpecting(
                                         getSchema(versionInfo.getOrdinal(), true)
                                                 .thenApply(schema -> new SchemaWithVersion(schema, versionInfo)), 
                                         e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, () -> {
                                            return getSchemas().thenApply(schemaRecords -> {
                                                if (schemaRecords.size() == 0) {
                                                    return null;
                                                }
                                                return schemaRecords.get(schemaRecords.size() - 1);
                                            });
                                         });
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String type) {
        LatestSchemaVersionForTypeKey key = new LatestSchemaVersionForTypeKey(type);
        return groupTable.getEntry(key, LatestSchemaVersionValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 return rec.getVersion();
                             }
                         })
                         .thenCompose(versionInfo -> {
                             if (versionInfo != null) {
                                 return Futures.exceptionallyComposeExpecting(
                                         getSchema(versionInfo.getOrdinal(), true)
                                         .thenApply(schema -> new SchemaWithVersion(schema, versionInfo)),
                                         e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, () -> {
                                             return getSchemas(type).thenApply(schemaRecords -> {
                                                 if (schemaRecords.size() == 0) {
                                                     return null;
                                                 }
                                                 return schemaRecords.get(schemaRecords.size() - 1);
                                             });
                                         });

                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }

    public CompletableFuture<List<String>> getCodecTypes() {
        return groupTable.getEntry(CODECS_TYPE_KEY, CodecTypesListValue.class)
                         .thenApply(codecTypes -> {
                             if (codecTypes == null) {
                                 return Collections.singletonList("");
                             } else {
                                 return codecTypes.getCodecTypes();
                             }
                         });
    }

    public CompletableFuture<Void> addCodecType(String codecType) {
        // get all codecTypes. if codec doesnt exist, add it to log. let it get synced to the table. 
        // generate encoding id will only generate if the codec is already registered.
        return WRITE_CONFLICT_RETRY.runAsync(() -> getCurrentEtag()
                .thenCompose(etag -> groupTable.getEntryWithVersion(CODECS_TYPE_KEY, CodecTypesListValue.class)
                                               .thenCompose(rec -> addCodecType(codecType, etag, rec))), executor);
    }

    private CompletionStage<Void> addCodecType(String codecType, Etag etag, GroupTable.Value<CodecTypesListValue, V> rec) {
        if (rec.getValue() == null || !rec.getValue().getCodecTypes().contains(codecType)) {
            List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new ArrayList<>();
            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));
            V version = rec.getVersion();
            List<String> codecTypes;
            if (rec.getValue() == null) {
                codecTypes = Collections.singletonList(codecType);
            } else {
                codecTypes = new LinkedList<>(rec.getValue().getCodecTypes());
                codecTypes.add(codecType);
            }
            CodecTypesListValue updated = new CodecTypesListValue(codecTypes);
            entries.add(new AbstractMap.SimpleEntry<>(CODECS_TYPE_KEY, new GroupTable.Value<>(updated, version)));

            return groupTable.updateEntries(entries);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<List<GroupHistoryRecord>> getHistory() {
        return getSchemaRecords(0).thenApply(schemaRecords -> {
                    return schemaRecords
                            .stream().map(x -> new GroupHistoryRecord(x.getSchemaInfo(), x.getVersionInfo(),
                                    x.getValidationRules(), x.getTimestamp(), getSchemaString(x.getSchemaInfo())))
                            .collect(Collectors.toList());
                });
    }
    
    public CompletableFuture<List<GroupHistoryRecord>> getHistory(String type) {
        return getHistory().thenApply(list ->
                list.stream().filter(x -> x.getSchema().getType().equals(type))
                    .collect(Collectors.toList()));
    }

    public CompletableFuture<VersionInfo> addSchema(SchemaInfo schemaInfo, GroupProperties prop, Etag etag) {
        List<TableKey> keys = new ArrayList<>();
        keys.add(LATEST_SCHEMA_VERSION_KEY);
        SchemaFingerprintKey schemaFingerprintKey = new SchemaFingerprintKey(getFingerprint(schemaInfo));
        keys.add(schemaFingerprintKey);

        keys.add(new LatestSchemaVersionForTypeKey(schemaInfo.getType()));
        keys.add(SCHEMA_TYPES_KEY);

        return groupTable.getEntriesWithVersion(keys, TableValue.class).thenCompose(values -> {
            LatestSchemaVersionValue latest = (LatestSchemaVersionValue) values.get(0).getValue();
            V latestVersion = values.get(0).getVersion();
            SchemaVersionList schemaIndex = (SchemaVersionList) values.get(1).getValue();
            V schemaIndexVersion = values.get(1).getVersion();
            int nextOrdinal = latest == null ? 0 : latest.getVersion().getOrdinal() + 1;
            int nextVersion;

            if (prop.isAllowMultipleTypes()) {
                LatestSchemaVersionValue objectLatestVersion = (LatestSchemaVersionValue) values.get(2).getValue();
                nextVersion = objectLatestVersion == null ? 0 : objectLatestVersion.getVersion().getVersion() + 1;
            } else {
                nextVersion = nextOrdinal;
            }
            VersionInfo next = new VersionInfo(schemaInfo.getType(), nextVersion, nextOrdinal);

            List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new LinkedList<>();
            // 0. etag
            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

            // 1. version info key. add
            entries.add(new AbstractMap.SimpleEntry<>(new SchemaTypeVersionKey(next.getType(), next.getVersion()),
                    new GroupTable.Value<>(new VersionOrdinalValue(next.getOrdinal()), null)));
            entries.add(new AbstractMap.SimpleEntry<>(new VersionKey(next.getOrdinal()),
                    new GroupTable.Value<>(new SchemaRecord(schemaInfo, next, prop.getSchemaValidationRules(), 
                            System.currentTimeMillis()), null)));

            // 2. schema info key. update
            List<VersionInfo> versions;
            if (schemaIndex == null) {
                versions = Collections.singletonList(next);
            } else {
                versions = new ArrayList<>(schemaIndex.getVersions());
                versions.add(next);
            }
            entries.add(new AbstractMap.SimpleEntry<>(schemaFingerprintKey,
                    new GroupTable.Value<>(new SchemaVersionList(versions), schemaIndexVersion)));

            // 3. latest schema version
            entries.add(new AbstractMap.SimpleEntry<>(LATEST_SCHEMA_VERSION_KEY,
                    new GroupTable.Value<>(new LatestSchemaVersionValue(next), latestVersion)));

            // 3.1 latest for object type
            V objectLatestVersionVersion = values.get(2).getVersion();
            entries.add(new AbstractMap.SimpleEntry<>(new LatestSchemaVersionForTypeKey(
                    schemaInfo.getType()),
                    new GroupTable.Value<>(new LatestSchemaVersionValue(next), objectLatestVersionVersion)));

            // 4. object types list
            SchemaTypesListValue typesValue = (SchemaTypesListValue) values.get(3).getValue();
            V typeVersion = values.get(3).getVersion();

            List<String> list = typesValue == null ? new ArrayList<>() :
                    Lists.newArrayList(typesValue.getTypes());
            if (!list.contains(schemaInfo.getType())) {
                list.add(schemaInfo.getType());
            }
            entries.add(new AbstractMap.SimpleEntry<>(SCHEMA_TYPES_KEY,
                    new GroupTable.Value<>(
                            new SchemaTypesListValue(list), typeVersion)));
            return groupTable.updateEntries(entries).thenApply(v -> next);
        });
    }

    public CompletableFuture<Void> updateValidationPolicy(SchemaValidationRules policy, Etag etag) {
        return groupTable.getEntryWithVersion(VALIDATION_POLICY_KEY, ValidationRecord.class)
                         .thenCompose(entry -> {
                             if (entry.getValue().getValidationRules().equals(policy)) {
                                 return CompletableFuture.completedFuture(null);
                             } else {
                                 List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new ArrayList<>();
                                 entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

                                 ValidationRecord updated = new ValidationRecord(policy);
                                 entries.add(new AbstractMap.SimpleEntry<>(VALIDATION_POLICY_KEY, new GroupTable.Value<>(updated, entry.getVersion())));

                                 return groupTable.updateEntries(entries);
                             }
                         });
    }

    public CompletableFuture<GroupProperties> getGroupProperties() {
        List<? extends TableKey> keys = Lists.newArrayList(GROUP_PROPERTY_KEY, VALIDATION_POLICY_KEY);
        return groupTable.getEntries(keys, TableValue.class)
                         .thenApply(entries -> {
                             GroupPropertiesRecord properties = (GroupPropertiesRecord) entries.get(0);
                             ValidationRecord validationRecord = (ValidationRecord) entries.get(1);
                             return new GroupProperties(properties.getSerializationFormat(), validationRecord.getValidationRules(),
                                     properties.isAllowMultipleTypes(),
                                     ImmutableMap.copyOf(properties.getProperties()));
                         });
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HashUtil.getFingerprint(schemaInfo.getSchemaData().array());
    }

    private CompletableFuture<EncodingId> generateNewEncodingId(VersionInfo versionInfo, String codecType, Etag etag) {
        return getSchema(versionInfo.getOrdinal(), true)
                .thenCompose(schema -> getCodecTypes()
                .thenCompose(codecTypes -> {
                    if (codecTypes.contains(codecType)) {
                        LatestEncodingIdKey key = new LatestEncodingIdKey();
                        return groupTable.getEntryWithVersion(key, LatestEncodingIdValue.class).thenCompose(current -> {
                            EncodingId nextEncodingId = current.getValue() == null ? new EncodingId(0) :
                                    new EncodingId(current.getValue().getEncodingId().getId() + 1);
                            V encodingIdVersion = current.getVersion();

                            List<Map.Entry<TableKey, GroupTable.Value<TableValue, V>>> entries = new LinkedList<>();

                            entries.add(new AbstractMap.SimpleEntry<>(ETAG, new GroupTable.Value<>(ETAG, groupTable.fromEtag(etag))));

                            EncodingIdRecord idIndex = new EncodingIdRecord(nextEncodingId);
                            EncodingInfoRecord infoIndex = new EncodingInfoRecord(versionInfo, codecType);
                            // add new entries for encoding id and info
                            entries.add(new AbstractMap.SimpleEntry<>(idIndex, new GroupTable.Value<>(infoIndex, null)));
                            entries.add(new AbstractMap.SimpleEntry<>(infoIndex, new GroupTable.Value<>(idIndex, null)));
                            // update
                            entries.add(new AbstractMap.SimpleEntry<>(LATEST_ENCODING_ID_KEY,
                                    new GroupTable.Value<>(new LatestEncodingIdValue(nextEncodingId), encodingIdVersion)));
                            return groupTable.updateEntries(entries)
                                             .thenApply(v -> nextEncodingId);
                        });
                    } else {
                        throw new CodecTypeNotRegisteredException(String.format("codec %s not registered", codecType));
                    }
                }));
    }

    public CompletableFuture<Either<EncodingId, Etag>> getEncodingId(VersionInfo versionInfo, String codecType) {
        EncodingInfoRecord encodingInfoIndex = new EncodingInfoRecord(versionInfo, codecType);
        return groupTable.getEntry(encodingInfoIndex, EncodingIdRecord.class)
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
            return Futures.exceptionallyExpecting(getSchema(version.getOrdinal(), true)
                    .thenAccept(schema -> {
                        if (Arrays.equals(schema.getSchemaData().array(), toFind.getSchemaData().array()) && schema.getType().equals(toFind.getType())) {
                            found.set(version);
                        }
                    }), e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
        }, executor).thenApply(v -> found.get());
    }

    @SneakyThrows
    private String getSchemaString(SchemaInfo schemaInfo) {
        String schemaString;
        switch (schemaInfo.getSerializationFormat()) {
            case Avro:
            case Json:
                schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                break;
            case Protobuf:
                DescriptorProtos.FileDescriptorSet descriptor = DescriptorProtos.FileDescriptorSet.parseFrom(schemaInfo.getSchemaData());
                JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames().usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().build());
                schemaString = printer.print(descriptor);
                break;
            default:
                schemaString = "";
                break;
        }
        return schemaString;
    }
}