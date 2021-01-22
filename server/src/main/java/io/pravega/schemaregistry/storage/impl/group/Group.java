/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.common.Either;
import io.pravega.schemaregistry.contract.data.CodecType;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupHistoryRecord;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.exceptions.CodecTypeNotRegisteredException;
import io.pravega.schemaregistry.service.Config;
import io.pravega.schemaregistry.storage.Etag;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.common.ChunkUtil;
import io.pravega.schemaregistry.storage.impl.group.records.TableRecords;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.schemaregistry.storage.impl.group.GroupTable.Value;
import static io.pravega.schemaregistry.storage.impl.group.GroupTable.Entry;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.*;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.CodecTypeKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.CodecTypeValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.LatestSchemasValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.LatestSchemasKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaIdKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaTypeValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.ValidationRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.VersionDeletedRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.EncodingIdRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.TableKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.EncodingInfoRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.GroupPropertiesRecord;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.CodecTypesListValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.LatestEncodingIdKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaFingerprintKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.GroupPropertyKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.IndexTypeVersionToIdKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaVersionList;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.CodecTypesKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.ValidationPolicyKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.LatestEncodingIdValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaIdValue;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaIdChunkKey;
import static io.pravega.schemaregistry.storage.impl.group.records.TableRecords.SchemaChunkRecord;

/**
 * Class that implements all storage business logic for a group.
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
    private static final LatestSchemasKey LATEST_SCHEMAS_KEY = new LatestSchemasKey();
    private static final LatestEncodingIdKey LATEST_ENCODING_ID_KEY = new LatestEncodingIdKey();
    private static final Retry.RetryAndThrowConditionally WRITE_CONFLICT_RETRY = 
            Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                 .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException);
    private static final CodecType NONE_CODEC_TYPE = new CodecType("");

    private final GroupTable<V> groupTable;
    private final ScheduledExecutorService executor;

    public Group(GroupTable<V> groupTable, ScheduledExecutorService executor) {
        this.groupTable = groupTable;
        this.executor = executor;
    }

    public CompletableFuture<Boolean> create(SerializationFormat serializationFormat, ImmutableMap<String, String> properties,
                                          boolean allowMultipleTypes, Compatibility compatibility) {
        List<Entry<V>> entries = new ArrayList<>();
        entries.add(new Entry<>(ETAG, ETAG, null));

        GroupPropertiesRecord groupProp = new GroupPropertiesRecord(serializationFormat, allowMultipleTypes, properties);
        entries.add(new Entry<>(GROUP_PROPERTY_KEY, groupProp, null));

        ValidationRecord validationRecord = new ValidationRecord(compatibility);
        entries.add(new Entry<>(VALIDATION_POLICY_KEY, validationRecord, null));

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
        return groupTable.getEntry(LATEST_SCHEMAS_KEY, LatestSchemasValue.class)
                         .thenCompose(types -> {
                             ImmutableMap<FormatAndType, SchemaTypeValue> schemas = types == null ? ImmutableMap.of() : types.getTypes();
                             List<SchemaIdKey> keys = schemas.values().stream().filter(x -> x.getLatestVersion() >= 0)
                                                             .map(x -> new SchemaIdKey(x.getLatestId()))
                                                             .collect(Collectors.toList());
                             
                             return groupTable.getEntries(keys, SchemaRecord.class)
                                     .thenCompose(entries -> Futures.allOfWithResults(entries
                                             .stream().map(x -> getSchemaInfo(x)
                                                     .thenApply(schemaInfo -> new SchemaWithVersion(schemaInfo,
                                                             new VersionInfo(x.getType(), x.getSerializationFormat().getFullTypeName(),
                                                                     x.getVersion(), x.getId()))))
                                             .collect(Collectors.toList())));
                         });
    }

    private CompletableFuture<SchemaInfo> getSchemaInfo(SchemaRecord sr) {
        if (sr.getSchemaInfo() != null) {
            return CompletableFuture.completedFuture(sr.getSchemaInfo());
        } else {
            List<SchemaIdChunkKey> keys = IntStream.range(1, sr.getNumberOfChunks()).boxed().map(y -> 
                    new SchemaIdChunkKey(sr.getId(), y)).collect(Collectors.toList());
            return groupTable.getEntries(keys, SchemaChunkRecord.class)
                    .thenApply(chunks -> {
                        List<ByteArraySegment> chunkList = new ArrayList<>();
                        chunkList.add(sr.getSchemaChunk());
                        chunks.forEach(x -> chunkList.add(x.getChunkPayload()));
                        return new SchemaInfo(sr.getType(), sr.getSerializationFormat(), ChunkUtil.combine(chunkList),
                                sr.getProperties());
                    });
        }
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas() {
        return getSchemas(0);
    }

    public CompletableFuture<List<SchemaWithVersion>> getSchemas(int fromPos) {
        return getSchemaRecords(fromPos)
                .thenApply(entries -> entries
                        .stream().map(x -> new SchemaWithVersion(x.getSchemaInfo(), 
                                new VersionInfo(x.getType(), x.getSerializationFormat().getFullTypeName(), x.getVersion(), x.getId())))
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
        return groupTable.getEntry(LATEST_SCHEMAS_KEY, LatestSchemasValue.class)
                         .thenCompose(latestSchemasValue -> {
                             if (latestSchemasValue == null) {
                                 return CompletableFuture.completedFuture(Collections.emptyList());
                             } else {
                                 int endPos = latestSchemasValue.getNextId();
                                 Set<Integer> deleted = latestSchemasValue.getDeletedIds();
                                 List<TableKey> keys = IntStream.range(fromPos, endPos)
                                                                .boxed().map(SchemaIdKey::new).collect(Collectors.toList());
                                 return groupTable.getEntriesWithVersion(keys, TableValue.class)
                                                  .thenCompose(entries -> {
                                                      List<SchemaRecord> schemaRecords = new ArrayList<>();

                                                      for (Value<TableValue, V> entry : entries) {
                                                          if (entry.getValue() instanceof SchemaRecord) {
                                                              schemaRecords.add((SchemaRecord) entry.getValue());
                                                          }
                                                      }
                                                      return Futures.allOfWithResults(schemaRecords
                                                              .stream().filter(x -> !deleted.contains(x.getId()))
                                                              .map(x -> getSchemaInfo(x)
                                                                      .thenApply(schemaInfo -> new SchemaRecord(
                                                                              schemaInfo, x.getId(), x.getVersion(),
                                                                              x.getCompatibility(), x.getTimestamp())))
                                                              .collect(Collectors.toList()));
                                                  });
                             }
                         });
    }

    private CompletableFuture<Integer> getSchemaId(String schemaType, int version, String serializationFormat) {
        return groupTable.getEntry(new IndexTypeVersionToIdKey(serializationFormat, schemaType, version), SchemaIdValue.class)
                .thenApply(x -> {
                    if (x != null) {
                        return x.getId();
                    } else {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("version not found %s, %s %s", serializationFormat, schemaType, version));
                    }
                });
    }

    public CompletableFuture<Void> deleteSchema(String schemaType, int version, String serializationFormat, Etag etag) {
        return getSchemaId(schemaType, version, serializationFormat)
                .thenCompose(id -> deleteSchema(id, etag));
    }
    
    public CompletableFuture<Void> deleteSchema(int id, Etag etag) {
        VersionDeletedRecord versionDeletedRecord = new VersionDeletedRecord(id);
        SchemaIdKey schemaIdKey = new SchemaIdKey(id);
        return groupTable.getEntriesWithVersion(Lists.newArrayList(schemaIdKey, versionDeletedRecord, LATEST_SCHEMAS_KEY), TableValue.class)
                .thenCompose(entries -> {
                    SchemaRecord schema = (SchemaRecord) entries.get(0).getValue();
                    TableValue versionDeletedRecordValue = entries.get(1).getValue();
                    if (schema == null) {
                        throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, String.format("version id found %s", id));
                    }
                    String serializationFormat = schema.getSerializationFormat().getFullTypeName();
                    String type = schema.getType();
                    LatestSchemasValue types = (LatestSchemasValue) entries.get(2).getValue();
                    SchemaTypeValue value = types.getTypes().entrySet().stream()
                                                 .filter(x -> x.getKey().getType().equals(type) && x.getKey().getSerializationFormat().equals(serializationFormat))
                                                 .findFirst().map(Map.Entry::getValue).orElse(null);

                    if (versionDeletedRecordValue == null) {
                        assert !types.getDeletedIds().contains(id);
                        List<Entry<V>> toUpdate = new ArrayList<>();
                        toUpdate.add(new Entry<>(ETAG, ETAG, groupTable.fromEtag(etag)));
                        toUpdate.add(new Entry<>(versionDeletedRecord, versionDeletedRecord, null));
                        // update latest version if the deleted version was the latest.
                        V typesVersion = entries.get(2).getVersion();
                        // if we are deleting the latest schema for the type, we need to update the latest too. 

                        // add schema id to version entry to the deleted versions map.
                        ImmutableSet.Builder<Integer> deletedIdsBuilder = new ImmutableSet.Builder<>();
                        deletedIdsBuilder.addAll(types.getDeletedIds());
                        deletedIdsBuilder.add(id);
                        ImmutableSet<Integer> deletedIds = deletedIdsBuilder.build();
                        ImmutableSet.Builder<Integer> deletedVersionsBuilder = new ImmutableSet.Builder<>();
                        deletedVersionsBuilder.addAll(value.getDeletedVersions());
                        deletedVersionsBuilder.add(schema.getVersion());
                        ImmutableSet<Integer> deletedVersions = deletedVersionsBuilder.build();

                        ImmutableMap.Builder<FormatAndType, SchemaTypeValue> newTypes = new ImmutableMap.Builder<>();
                        for (Map.Entry<FormatAndType, SchemaTypeValue> entry : types.getTypes().entrySet()) {
                            if (!(entry.getKey().getType().equals(type) && entry.getKey().getSerializationFormat().equals(serializationFormat))) {
                                newTypes.put(entry.getKey(), entry.getValue());
                            }
                        }

                        CompletableFuture<SchemaTypeValue> newValueFuture;
                        if (id != value.getLatestId()) {
                            newValueFuture = CompletableFuture.completedFuture(
                                    new SchemaTypeValue(value.getLatestVersion(), value.getLatestId(), value.getNextVersion(), deletedVersions));
                        } else {
                            // we are deleting the latest schema for the type.. we need to find the previous non deleted schema
                            // for the type
                            // first find the highest non deleted version number for the type.  
                            AtomicInteger previous = new AtomicInteger(value.getLatestVersion() - 1);
                            while (deletedVersions.contains(previous.get())) {
                                previous.decrementAndGet();
                            }
                            
                            if (previous.get() < 0) {
                                // if previous latest is less than 0, set the latest id and versions as -1
                                newValueFuture = CompletableFuture.completedFuture(
                                        new SchemaTypeValue(previous.get(), previous.get(), value.getNextVersion(), deletedVersions));
                            } else {
                                // if we have found the previous non deleted version, find the corresponding id for the latest schema version 
                                // and set the id and version into the new schema value type to be updated.
                                newValueFuture = getSchemaId(type, previous.get(), serializationFormat)
                                        .thenApply(i -> new SchemaTypeValue(previous.get(), i, value.getNextVersion(), deletedVersions));
                            }
                        }
                        return newValueFuture.thenCompose(n -> {
                            newTypes.put(new FormatAndType(serializationFormat, type), n);
                            toUpdate.add(new Entry<>(LATEST_SCHEMAS_KEY, 
                                    new LatestSchemasValue(newTypes.build(), types.getNextId(), deletedIds), typesVersion));
                            return groupTable.updateEntries(toUpdate);
                                });
                    } else {
                        // already deleted. Idempotent case. 
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<SchemaInfo> getSchema(String schemaType, int version, String serializationFormat) {
        return getSchemaId(schemaType, version, serializationFormat)
                .thenCompose(id -> getSchema(id, false));
    }

    public CompletableFuture<SchemaInfo> getSchema(int id) {
        return getSchema(id, false);
    }

    private CompletableFuture<SchemaInfo> getSchema(int id, boolean throwOnDeleted) {
        List<? extends TableKey> keys = Lists.newArrayList(new SchemaIdKey(id),
                new VersionDeletedRecord(id));
        return groupTable.getEntriesWithVersion(keys, TableValue.class)
                         .thenCompose(entries -> {
                             if (entries.get(1).getValue() != null && throwOnDeleted) {
                                 throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, Integer.toString(id));
                             } else {
                                 TableValue value = entries.get(0).getValue();
                                 if (value != null) {
                                     return getSchemaInfo((SchemaRecord) value);
                                 } else {
                                     throw StoreExceptions.create(StoreExceptions.Type.DATA_NOT_FOUND, "schema version not found");
                                 }
                             }
                         });
    }

    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo, BigInteger fingerprint) {
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
                             CompletableFuture<SchemaInfo> schemaFuture = getSchema(encodingInfo.getVersionInfo().getId());
                             CompletableFuture<CodecType> codecFuture = getCodecType(encodingInfo.getCodecType());
                             return CompletableFuture.allOf(schemaFuture, codecFuture)
                                     .thenApply(v -> {
                                         SchemaInfo schemaInfo = schemaFuture.join();
                                         CodecType codecType = codecFuture.join();
                                         return new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, codecType);
                                     });
                         });
    }

    private CompletableFuture<CodecType> getCodecType(String codecType) {
        if (codecType.length() == 0) {
            return CompletableFuture.completedFuture(NONE_CODEC_TYPE);   
        } else {
            CodecTypeKey key = new CodecTypeKey(codecType);
            return groupTable.getEntry(key, CodecTypeValue.class)
                             .thenApply(entry -> new CodecType(codecType, entry.getProtperties()));
        }
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion() {
        return groupTable.getEntry(LATEST_SCHEMAS_KEY, LatestSchemasValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 return rec.getTypes().entrySet().stream().filter(x -> x.getValue().getLatestVersion() >= 0)
                                           .max(Comparator.comparingInt(x -> x.getValue().getLatestId()))
                                         .orElse(null);
                             }
                         })
                         .thenCompose(schemaTypeValue -> {
                             if (schemaTypeValue != null) {
                                 return getSchema(schemaTypeValue.getValue().getLatestId(), true)
                                                 .thenApply(schema -> new SchemaWithVersion(schema, new VersionInfo(schema.getType(),
                                                         schemaTypeValue.getKey().getSerializationFormat(),
                                                         schemaTypeValue.getValue().getLatestVersion(), schemaTypeValue.getValue().getLatestId()))); 
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }
    
    public CompletableFuture<SchemaWithVersion> getLatestSchemaVersion(String type) {
        return groupTable.getEntry(LATEST_SCHEMAS_KEY, LatestSchemasValue.class)
                         .thenApply(rec -> {
                             if (rec == null) {
                                 return null;
                             } else {
                                 SchemaTypeValue value = rec.getTypes().entrySet().stream()
                                      .filter(x -> x.getKey().getType().equals(type))
                                      .max(Comparator.comparingInt(x -> x.getValue().getLatestVersion()))
                                      .map(Map.Entry::getValue).orElse(null);

                                 return value == null || value.getLatestVersion() < 0 ? null : value;
                             }
                         })
                         .thenCompose(schemaTypeValue -> {
                             if (schemaTypeValue != null) {
                                 return getSchema(schemaTypeValue.getLatestId(), true)
                                         .thenApply(schema -> new SchemaWithVersion(schema,
                                                 new VersionInfo(schema.getType(), schema.getSerializationFormat().getFullTypeName(),
                                                 schemaTypeValue.getLatestVersion(), schemaTypeValue.getLatestId())));
                             } else {
                                 return CompletableFuture.completedFuture(null);
                             }
                         });
    }

    public CompletableFuture<List<CodecType>> getCodecTypes() {
        return getCodecTypeNames()
                .thenCompose(names -> 
                        Futures.allOfWithResults(names.stream().map(this::getCodecType).collect(Collectors.toList())));
    }

    private CompletableFuture<List<String>> getCodecTypeNames() {
        return groupTable.getEntry(CODECS_TYPE_KEY, CodecTypesListValue.class)
                         .thenApply(codecTypes -> {
                             if (codecTypes == null) {
                                 return Collections.emptyList();
                             } else {
                                 return codecTypes.getCodecTypes();
                             }
                         });
    }

    public CompletableFuture<Void> addCodecType(CodecType codecType) {
        // get all codecTypes. if codec doesnt exist, add it to the list of codecs. 
        // generate encoding id will only generate if the codec is already registered.
        return WRITE_CONFLICT_RETRY.runAsync(() -> getCurrentEtag()
                .thenCompose(etag -> groupTable.getEntryWithVersion(CODECS_TYPE_KEY, CodecTypesListValue.class)
                                               .thenCompose(rec -> addCodecType(codecType, etag, rec))), executor);
    }

    private CompletionStage<Void> addCodecType(CodecType codecType, Etag etag, Value<CodecTypesListValue, V> rec) {
        if (rec.getValue() == null || !rec.getValue().getCodecTypes().contains(codecType.getName())) {
            List<Entry<V>> entries = new ArrayList<>();
            entries.add(new Entry<>(ETAG, ETAG, groupTable.fromEtag(etag)));
            V version = rec.getVersion();
            ImmutableList.Builder<String> codecTypesBuilder = new ImmutableList.Builder<>();
            if (rec.getValue() == null) {
                codecTypesBuilder.add(codecType.getName());
            } else {
                codecTypesBuilder.addAll(rec.getValue().getCodecTypes());
                codecTypesBuilder.add(codecType.getName());
            }
            CodecTypesListValue updated = new CodecTypesListValue(codecTypesBuilder.build());
            entries.add(new Entry<>(CODECS_TYPE_KEY, updated, version));
            entries.add(new Entry<>(new CodecTypeKey(codecType.getName()), new CodecTypeValue(codecType.getProperties()), null));

            return groupTable.updateEntries(entries);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<List<GroupHistoryRecord>> getHistory() {
        return getSchemaRecords(0).thenApply(schemaRecords -> schemaRecords
                .stream().map(x -> new GroupHistoryRecord(x.getSchemaInfo(), 
                        new VersionInfo(x.getType(), x.getSerializationFormat().getFullTypeName(), x.getVersion(), x.getId()), 
                        x.getCompatibility(), x.getTimestamp(), getSchemaString(x.getSchemaInfo())))
                .collect(Collectors.toList()));
    }
    
    public CompletableFuture<List<GroupHistoryRecord>> getHistory(String type) {
        return getHistory().thenApply(list ->
                list.stream().filter(x -> x.getSchemaInfo().getType().equals(type))
                    .collect(Collectors.toList()));
    }

    public CompletableFuture<VersionInfo> addSchema(SchemaInfo schemaInfo, BigInteger fingerprint, GroupProperties prop, Etag etag) {
        List<TableKey> keys = new ArrayList<>();
        keys.add(LATEST_SCHEMAS_KEY);
        SchemaFingerprintKey schemaFingerprintKey = new SchemaFingerprintKey(fingerprint);
        keys.add(schemaFingerprintKey);

        // add or upadte following entries:
        // 0. etag
        // 1. schemaId -> record, index for type+verion -> id
        // 2. schemaIndex/fingerprint -> schema version list
        // 3. latest schema types for this new schema (add or update)
        return groupTable.getEntriesWithVersion(keys, TableValue.class).thenCompose(values -> {
            LatestSchemasValue schemaTypes = (LatestSchemasValue) values.get(0).getValue();
            V schemaTypesVersion = values.get(0).getVersion();
            SchemaVersionList schemaVersionList = (SchemaVersionList) values.get(1).getValue();
            V schemaIndexVersion = values.get(1).getVersion();
            // add or update schema types 
            // 1. get and update the next ordinal
            // 2. get and update the type specific next version
            int nextOrdinal;
            ImmutableSet<Integer> deletedSet;
            SchemaTypeValue schemaTypeValue;

            if (schemaTypes == null) {
                nextOrdinal = 0;
                deletedSet = ImmutableSet.of();
                schemaTypeValue = null;
            } else {
                nextOrdinal = schemaTypes.getNextId();
                deletedSet = schemaTypes.getDeletedIds();
                schemaTypeValue = schemaTypes.getTypes().entrySet().stream()
                                             .filter(x -> x.getKey().getType().equals(schemaInfo.getType()) 
                                                     && x.getKey().getSerializationFormat().equals(schemaInfo.getSerializationFormat().getFullTypeName()))
                                             .findFirst().map(Map.Entry::getValue).orElse(null);
            }
            int nextVersion;
            ImmutableSet<Integer> deletedVersions;
            if (schemaTypeValue == null) {
                nextVersion = 0;
                deletedVersions = ImmutableSet.of();
            } else {
                nextVersion = schemaTypeValue.getNextVersion();
                deletedVersions = schemaTypeValue.getDeletedVersions();
            }
            VersionInfo next = new VersionInfo(schemaInfo.getType(), schemaInfo.getSerializationFormat().getFullTypeName(), 
                    nextVersion, nextOrdinal);

            List<Entry<V>> entries = new LinkedList<>();
            // 0. etag
            entries.add(new Entry<>(ETAG, ETAG, groupTable.fromEtag(etag)));
            // 1. Schema id to schema record
            // 1.1 index for version to id
            String serializationFormat = schemaInfo.getSerializationFormat().getFullTypeName();
            entries.add(new Entry<>(new IndexTypeVersionToIdKey(serializationFormat,
                    next.getType(), next.getVersion()),
                    new SchemaIdValue(next.getId()), null));
            // break schema binary into smaller chunks.
            List<ByteArraySegment> chunks = ChunkUtil.chunk(schemaInfo.getSchemaData(), Config.MAX_CHUNK_SIZE_BYTES);
            entries.add(new Entry<>(new SchemaIdKey(next.getId()),
                    SchemaRecord.builder()
                                .type(schemaInfo.getType())
                                .serializationFormat(schemaInfo.getSerializationFormat())
                                .properties(schemaInfo.getProperties())
                                .schemaChunk(chunks.get(0))
                                .id(next.getId())
                                .version(next.getVersion())
                                .compatibility(prop.getCompatibility())
                                .timestamp(System.currentTimeMillis())
                                .maxChunkSize(Config.MAX_CHUNK_SIZE_BYTES)
                                .numberOfChunks(chunks.size())
                                .build(), null));

            // Start from chunk 1 because we have already included chunk 0 in the schema record
            for (int i = 1; i < chunks.size(); i++) { 
                entries.add(new Entry<>(new SchemaIdChunkKey(next.getId(), i),
                        new SchemaChunkRecord(chunks.get(i)), null));
            } 

            // 2. Schema fingerprint key
            ImmutableList.Builder<VersionInfo> versionsBuilder = new ImmutableList.Builder<>();
            if (schemaVersionList != null) {
                versionsBuilder.addAll(new ArrayList<>(schemaVersionList.getVersions()));
            }
            versionsBuilder.add(next);
            ImmutableList<VersionInfo> versions = versionsBuilder.build();
            entries.add(new Entry<>(schemaFingerprintKey,
                    new SchemaVersionList(versions), schemaIndexVersion));

            // 3. add to latest schemas which updates the latest and next versions for the schema type
            // and next id for overall group
            ImmutableMap.Builder<FormatAndType, SchemaTypeValue> builder = ImmutableMap.builder();
            if (schemaTypes != null) {
                for (Map.Entry<FormatAndType, SchemaTypeValue> entry : schemaTypes.getTypes().entrySet()) {
                    if (!(entry.getKey().getSerializationFormat().equals(serializationFormat) 
                            && entry.getKey().getType().equals(schemaInfo.getType()))) {
                        builder.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            SchemaTypeValue newValue = new SchemaTypeValue(nextVersion, nextOrdinal, 
                    nextVersion + 1, deletedVersions);
            builder.put(new FormatAndType(serializationFormat, schemaInfo.getType()), newValue);
            
            entries.add(new Entry<>(LATEST_SCHEMAS_KEY,
                    new LatestSchemasValue(builder.build(), nextOrdinal + 1, deletedSet), schemaTypesVersion));

            return groupTable.updateEntries(entries).thenApply(v -> next);
        });
    }

    public CompletableFuture<Void> updateValidationPolicy(Compatibility policy, Etag etag) {
        return groupTable.getEntryWithVersion(VALIDATION_POLICY_KEY, ValidationRecord.class)
                         .thenCompose(entry -> {
                             if (entry.getValue().getCompatibility().equals(policy)) {
                                 return CompletableFuture.completedFuture(null);
                             } else {
                                 List<Entry<V>> entries = new ArrayList<>();
                                 entries.add(new Entry<>(ETAG, ETAG, groupTable.fromEtag(etag)));

                                 ValidationRecord updated = new ValidationRecord(policy);
                                 entries.add(new Entry<>(VALIDATION_POLICY_KEY, updated, entry.getVersion()));

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
                             return new GroupProperties(properties.getSerializationFormat(), validationRecord.getCompatibility(),
                                     properties.isAllowMultipleTypes(),
                                     ImmutableMap.copyOf(properties.getProperties()));
                         });
    }

    private CompletableFuture<EncodingId> generateNewEncodingId(VersionInfo versionInfo, String codecType, Etag etag) {
        return getSchema(versionInfo.getId(), true)
                .thenCompose(schema -> getCodecTypeNames()
                .thenCompose(codecTypes -> {
                    if (codecType.length() == 0 || codecTypes.contains(codecType)) {
                        LatestEncodingIdKey key = new LatestEncodingIdKey();
                        return groupTable.getEntryWithVersion(key, LatestEncodingIdValue.class).thenCompose(current -> {
                            EncodingId nextEncodingId = current.getValue() == null ? new EncodingId(0) :
                                    new EncodingId(current.getValue().getEncodingId().getId() + 1);
                            V encodingIdVersion = current.getVersion();

                            List<Entry<V>> entries = new LinkedList<>();

                            entries.add(new Entry<>(ETAG, ETAG, groupTable.fromEtag(etag)));

                            EncodingIdRecord idIndex = new EncodingIdRecord(nextEncodingId);
                            EncodingInfoRecord infoIndex = new EncodingInfoRecord(versionInfo, codecType);
                            // add new entries for encoding id and info
                            entries.add(new Entry<>(idIndex, infoIndex, null));
                            entries.add(new Entry<>(infoIndex, idIndex, null));
                            // update
                            entries.add(new Entry<>(LATEST_ENCODING_ID_KEY, new LatestEncodingIdValue(nextEncodingId), encodingIdVersion));
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
        return Futures.loop(() -> iterator.hasNext() && found.get() == null, () -> {
            VersionInfo version = iterator.next();
            return Futures.exceptionallyExpecting(getSchema(version.getId(), true)
                    .thenAccept(schema -> {
                        // Do note that we store the user supplied schema in its original avatar. While the fingerprint
                        // is computed on the normalized form. So when we fetch the schemas that have identical fingerprints,
                        // we will only compare type name and format and declare two schemas equal. 
                        // We can do this with fair confidence because we use the sha 256 hash for fingerprints. 
                        // The probability of collision on two non identical byte arrays to produce same fingerprint
                        // is next to impossible. 
                        // However, since we also deal with composite schemas (e.g. protobuf file descriptor set or avro union)
                        // where same schema could include definition for multiple objects and the type name distinguishes
                        // different entities, we will still need to compare type and format if the schema binary has identical
                        // fingerprint before we consider two schemas to be identical. 
                        if (schema.getType().equals(toFind.getType()) && schema.getSerializationFormat().equals(toFind.getSerializationFormat())) {
                            found.set(version);
                        }
                    }), e -> Exceptions.unwrap(e) instanceof StoreExceptions.DataNotFoundException, null);
        }, executor).thenApply(v -> found.get());
    }

    private String getSchemaString(SchemaInfo schemaInfo) {
        String schemaString;
        switch (schemaInfo.getSerializationFormat()) {
            case Avro:
            case Json:
                schemaString = new String(schemaInfo.getSchemaData().array(), Charsets.UTF_8);
                break;
            case Protobuf:
                try {
                    DescriptorProtos.FileDescriptorSet descriptor = DescriptorProtos.FileDescriptorSet.parseFrom(schemaInfo.getSchemaData());
                    JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames().usingTypeRegistry(JsonFormat.TypeRegistry.newBuilder().build());

                    schemaString = printer.print(descriptor);
                } catch (InvalidProtocolBufferException e) {
                    log.warn("unable to convert protobuf schema to json string", e);
                    throw new IllegalArgumentException(e);
                }
                break;
            default:
                schemaString = "";
                break;
        }
        return schemaString;
    }
}