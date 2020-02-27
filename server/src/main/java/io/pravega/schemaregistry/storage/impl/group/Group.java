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

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaEvolutionEpoch;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaType;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.IndexRecord;
import io.pravega.schemaregistry.storage.records.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Group<T> {
    private static final IndexRecord.SyncdTillKey SYNCD_TILL = new IndexRecord.SyncdTillKey();
    private static final IndexRecord.ValidationPolicyKey VALIDATION_POLICY_INDEX_KEY = new IndexRecord.ValidationPolicyKey();
    private static final IndexRecord.GroupPropertyKey GROUP_PROPERTY_INDEX_KEY = new IndexRecord.GroupPropertyKey();
    
    private static final Comparator<Index.Entry> VERSION_COMPARATOR = (v1, v2) -> {
        IndexRecord.VersionInfoKey version1 = (IndexRecord.VersionInfoKey) v1.getKey();
        IndexRecord.VersionInfoKey version2 = (IndexRecord.VersionInfoKey) v2.getKey();
        return Integer.compare(version1.getVersionInfo().getVersion(), version2.getVersionInfo().getVersion());
    };
    private static final Comparator<Index.Entry> ENCODING_ID_COMPARATOR = (v1, v2) -> {
        IndexRecord.EncodingIdIndex id1 = (IndexRecord.EncodingIdIndex) v1.getKey();
        IndexRecord.EncodingIdIndex id2 = (IndexRecord.EncodingIdIndex) v2.getKey();
        return Integer.compare(id1.getEncodingId().getId(), id2.getEncodingId().getId());
    };
    private static final HashFunction HASH = Hashing.murmur3_128();

    private final Log wal;

    private final Index<T> index;
    
    private final ScheduledExecutorService executor;

    public Group(Log wal, Index<T> index, ScheduledExecutorService executor) {
        this.wal = wal;
        this.index = index;
        this.executor = executor;
    }
    
    public CompletableFuture<Void> create(SchemaType schemaType, boolean enableEncoding, boolean subgroupByEventType, SchemaValidationRules schemaValidationRules) {
        return wal.writeToLog(new Record.GroupPropertiesRecord(schemaType, enableEncoding, subgroupByEventType, schemaValidationRules), null)
                  .thenCompose(pos -> {
                      IndexRecord.WALPositionValue walPosition = new IndexRecord.WALPositionValue(pos);
                      Operation.Add addGroupProp = new Operation.Add(GROUP_PROPERTY_INDEX_KEY, walPosition);
                      Operation.Add addSyncTill = new Operation.Add(SYNCD_TILL, walPosition);
                      return updateIndex(Lists.newArrayList(addGroupProp, addSyncTill));
                  });
    }
    
    @SuppressWarnings("unchecked")
    public CompletableFuture<Position> sync() {
        return index.getRecordWithVersion(SYNCD_TILL, IndexRecord.WALPositionValue.class)
             .thenCompose(value -> {
                 // TODO: handle if syncd till is not set
                 IndexRecord.WALPositionValue syncdTill = value.getValue();
                 return wal.readFrom(syncdTill.getPosition())
                         .thenCompose(list -> {
                             List<Operation> operations = new LinkedList<>();
                             list.forEach(x -> {
                                 if (x.getRecord() instanceof Record.SchemaRecord) {
                                     Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                                     Operation.Add add = new Operation.Add(new IndexRecord.VersionInfoKey(record.getVersionInfo()), new IndexRecord.WALPositionValue(x.getPosition()));
                                     Operation.AddToList addToList = new Operation.AddToList(new IndexRecord.SchemaInfoKey(getFingerprint(record.getSchemaInfo())),
                                             new IndexRecord.SchemaVersionValue(Collections.singletonList(record.getVersionInfo())));
                                     operations.add(add);
                                     operations.add(addToList);
                                 } else if (x.getRecord() instanceof Record.EncodingRecord) {
                                     Record.EncodingRecord record = (Record.EncodingRecord) x.getRecord();
                                     IndexRecord.EncodingIdIndex idIndex = new IndexRecord.EncodingIdIndex(record.getEncodingId());
                                     IndexRecord.EncodingInfoIndex infoIndex = new IndexRecord.EncodingInfoIndex(record.getVersionInfo(), record.getCompressionType());
                                     Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
                                     Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
                                     operations.add(idToInfo);
                                     operations.add(infoToId);
                                 } else if (x.getRecord() instanceof Record.ValidationRecord) {
                                     Operation.GetAndSet getAndSet = new Operation.GetAndSet(new IndexRecord.ValidationPolicyKey(),
                                             new IndexRecord.WALPositionValue(x.getPosition()),
                                             m -> ((IndexRecord.WALPositionValue) m).getPosition().getPosition().compareTo(x.getPosition().getPosition()) < 0);
                                     operations.add(getAndSet);
                                 }
                             });
                             return updateIndex(operations)
                                     .thenApply(v -> {
                                         if (list.isEmpty()) {
                                             return syncdTill.getPosition();
                                         } else {
                                             return list.get(list.size() - 1).getPosition();
                                         }
                                     });
                         });
             });
    }

    public CompletableFuture<SchemaValidationRules> getCurrentValidationRules() {
        return sync()
                .thenCompose(v -> index.getRecord(VALIDATION_POLICY_INDEX_KEY, IndexRecord.WALPositionValue.class)
                                       .thenCompose(validationRecordPosition -> {
                                           if (validationRecordPosition == null) {
                                               return index.getRecord(GROUP_PROPERTY_INDEX_KEY, IndexRecord.WALPositionValue.class)
                                                           .thenCompose(groupPropPos -> {
                                                              return wal.readAt(groupPropPos.getPosition(), Record.GroupPropertiesRecord.class)
                                                              .thenApply(Record.GroupPropertiesRecord::getValidationRules);        
                                                           });
                                               
                                           } else {
                                               return wal.readAt(validationRecordPosition.getPosition(), Record.ValidationRecord.class)
                                                                                         .thenApply(Record.ValidationRecord::getValidationRules);
                                           }
                                       }));
    }

    public CompletableFuture<Position> getCurrentEtag() {
        return wal.getCurrentEtag();
    }

    public CompletableFuture<Position> writeToLog(Record record, Position etag) {
        return wal.writeToLog(record, etag);
    }

    public CompletableFuture<ListWithToken<String>> getSubgroups() {
        return getGroupProperties()
                .thenCompose(groupProperties -> {
                    if (!groupProperties.isSubgroupBySchemaName()) {
                        return Futures.failedFuture(new IllegalStateException());
                    }
                    // get all index keys
                    return index.getAllKeys()
                                .thenApply(list -> {
                                    List<String> subgroups = list
                                            .stream().filter(x -> x instanceof IndexRecord.VersionInfoKey)
                                            .map(x -> ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName())
                                            .distinct().collect(Collectors.toList());
                                    return new ListWithToken<>(subgroups, null);
                                });
                });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas() {
        return wal.readFrom(null)
                .thenApply(records -> {
                    List<SchemaWithVersion> schemas = records.stream().filter(x -> x.getRecord() instanceof Record.SchemaRecord).map(x -> {
                        Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                        return new SchemaWithVersion(record.getSchemaInfo(), record.getVersionInfo());
                    }).collect(Collectors.toList());

                    return new ListWithToken<>(schemas, null);
                });
    }

    public CompletableFuture<ListWithToken<SchemaWithVersion>> getSchemas(String subgroup) {
        return wal.readFrom(null)
                .thenApply(records -> {
                    List<SchemaWithVersion> schemas = records.stream().filter(x -> x.getRecord() instanceof Record.SchemaRecord
                            && ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(subgroup)).map(x -> {
                        Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                        return new SchemaWithVersion(record.getSchemaInfo(), record.getVersionInfo());
                    }).collect(Collectors.toList());

                    return new ListWithToken<>(schemas, null);
                });
    }

    public CompletableFuture<SchemaInfo> getSchema(VersionInfo versionInfo) {
        return index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class)
                .thenCompose(record -> {
                    if (record == null) {
                        return sync().thenCompose(v -> 
                                index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class));
                    } else {
                        return CompletableFuture.completedFuture(record);
                    }
                }).thenCompose(record -> wal.readAt(record.getPosition(), Record.SchemaRecord.class))
                    .thenApply(Record.SchemaRecord::getSchemaInfo);
    }
    
    public CompletableFuture<VersionInfo> getVersion(SchemaInfo schemaInfo) {
        long fingerPrint = getFingerprint(schemaInfo);
        return getVersionInternal(schemaInfo, fingerPrint)
                .thenCompose(found -> {
                    if (found == null) {
                        // sync and search again
                        return sync().thenCompose(v -> getVersionInternal(schemaInfo, fingerPrint));
                    } else {
                        return CompletableFuture.completedFuture(found);
                    }
                })
                .thenApply(version -> {
                    if (version == null) {
                        throw new StoreExceptions.DataNotFoundException();
                    } else {
                        return version;
                    }
                });
    }
    
    private CompletableFuture<VersionInfo> getVersionInternal(SchemaInfo schemaInfo, long fingerprint) {
        IndexRecord.SchemaInfoKey key = new IndexRecord.SchemaInfoKey(fingerprint);

        return index.getRecord(key, IndexRecord.SchemaVersionValue.class)
                    .thenCompose(record -> {
                        if (record != null) {
                            return findVersion(record.getVersions(), schemaInfo)
                                    .thenApply(found -> {
                                        if (found != null) {
                                            return found;
                                        } else {
                                            return null;
                                        }
                                    });

                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
    }

    public CompletableFuture<VersionInfo> findVersion(List<VersionInfo> versions, SchemaInfo toFind) {
        AtomicReference<VersionInfo> found = new AtomicReference<>();
        Iterator<VersionInfo> iterator = versions.iterator();
        return Futures.loop(() -> {
            return iterator.hasNext() && found.get() == null;
        }, () -> {
            VersionInfo version = iterator.next();
            return getSchema(version)
                    .thenAccept(schema -> {
                        if (Arrays.equals(schema.getSchemaData(), toFind.getSchemaData())) {
                            found.set(version);
                        }
                    });
        }, executor)
                .thenApply(v -> found.get());
    }

    public CompletableFuture<EncodingId> getOrCreateEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        return getEncodingId(versionInfo, compressionType)
                .thenCompose(encodingId -> {
                    if (encodingId == null) {
                        return sync().thenCompose(position -> getEncodingId(versionInfo, compressionType)
                                     .thenCompose(encodingIdSyncd -> {
                                         if (encodingIdSyncd == null) {
                                             return generateNewEncodingId(versionInfo, compressionType, position);
                                         } else {
                                             return CompletableFuture.completedFuture(encodingIdSyncd);
                                         }
                                     }));
                    } else {
                        return CompletableFuture.completedFuture(encodingId);
                    }
                });
    }

    private CompletableFuture<EncodingId> generateNewEncodingId(VersionInfo versionInfo, CompressionType compressionType, Position position) {
        return getNextEncodingId()
                .thenCompose(id -> writeToLog(new Record.EncodingRecord(id, versionInfo, compressionType), position)
                        .thenCompose(v -> {
                            IndexRecord.EncodingIdIndex idIndex = new IndexRecord.EncodingIdIndex(id);
                            IndexRecord.EncodingInfoIndex infoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, compressionType);
                            Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
                            Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
                            return updateIndex(Lists.newArrayList(idToInfo, infoToId));
                        }).thenApply(v -> id));
    }

    public CompletableFuture<EncodingId> getEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        IndexRecord.EncodingInfoIndex encodingInfoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, compressionType);
        return index.getRecord(encodingInfoIndex, IndexRecord.EncodingIdIndex.class)
             .thenApply(record -> record == null ? null : record.getEncodingId());
    }

    public CompletableFuture<EncodingInfo> getEncodingInfo(EncodingId encodingId) {
        IndexRecord.EncodingIdIndex encodingIdIndex = new IndexRecord.EncodingIdIndex(encodingId);
        return index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class)
             .thenCompose(encodingInfo -> {
                 if (encodingInfo == null) {
                     return sync()
                             .thenCompose(v -> index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class)
                                                .thenApply(info -> {
                                         if (info == null) {
                                             throw new StoreExceptions.DataNotFoundException();
                                         } else {
                                             return info;
                                         }
                                     }));
                 } else {
                     return CompletableFuture.completedFuture(encodingInfo);
                 }
             }).thenCompose(encodingInfo -> getSchema(encodingInfo.getVersionInfo())
                .thenApply(schemaInfo -> new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, encodingInfo.getCompressionType())));
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema() {
        Predicate<IndexRecord.IndexKey> versionPredicate = x -> x instanceof IndexRecord.VersionInfoKey;

        return getLatestEntryFor(versionPredicate, VERSION_COMPARATOR)
                .thenCompose(max -> {
                    if (max != null) {
                        IndexRecord.VersionInfoKey key = (IndexRecord.VersionInfoKey) max.getKey();
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<SchemaWithVersion> getLatestSchema(String subgroup) {
        Predicate<IndexRecord.IndexKey> versionForSubgroup = x -> x instanceof IndexRecord.VersionInfoKey &&
                ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(subgroup);

        return getLatestEntryFor(versionForSubgroup, VERSION_COMPARATOR)
                .thenCompose(entry -> {
                    if (entry == null) {
                        return null;
                    } else {
                        IndexRecord.VersionInfoKey key = (IndexRecord.VersionInfoKey) entry.getKey();
                        VersionInfo version = key.getVersionInfo();
                        return getSchema(version).thenApply(schema -> new SchemaWithVersion(schema, version));
                    }
                });
    }

    public CompletableFuture<List<CompressionType>> getCompressions() {
        return sync()
                .thenCompose(v -> {
                    Predicate<IndexRecord.IndexKey> encodingInfoPredicate = x -> x instanceof IndexRecord.EncodingInfoIndex;
                    return index.getAllEntries(encodingInfoPredicate)
                                .thenApply(list -> {
                                    return list.stream().map(x -> {
                                        IndexRecord.EncodingInfoIndex encodingInfoIndex = (IndexRecord.EncodingInfoIndex) x.getKey();
                                        return encodingInfoIndex.getCompressionType();
                                    }).distinct().collect(Collectors.toList());
                                });
                });
    }

    public CompletableFuture<List<SchemaEvolutionEpoch>> getHistory() {
        AtomicReference<SchemaValidationRules> rulesRef = new AtomicReference<>();
        List<SchemaEvolutionEpoch> epochs = new LinkedList<>();
        return wal.readFrom(null)
                  .thenApply(list -> {
                      list.forEach(x -> {
                          if (x.getRecord() instanceof Record.SchemaRecord) {
                              Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                              SchemaEvolutionEpoch epoch = new SchemaEvolutionEpoch(record.getSchemaInfo(), record.getVersionInfo(),
                                      rulesRef.get());
                              epochs.add(epoch);
                          } else if (x.getRecord() instanceof Record.ValidationRecord) {
                              rulesRef.set(((Record.ValidationRecord) x.getRecord()).getValidationRules());
                          } else if (x.getRecord() instanceof Record.GroupPropertiesRecord) {
                              rulesRef.set(((Record.GroupPropertiesRecord) x.getRecord()).getValidationRules());
                          }
                      });
                      return epochs;
                  });
    }

    public CompletableFuture<List<SchemaEvolutionEpoch>> getHistory(String subgroup) {
        return getHistory().thenApply(list -> 
                list.stream().filter(x -> x.getSchema().getName().equals(subgroup)).collect(Collectors.toList()));
    }

    public CompletableFuture<VersionInfo> addSchemaToGroup(SchemaInfo schemaInfo, Position etag) {
        Predicate<IndexRecord.IndexKey> versionPredicate = x -> x instanceof IndexRecord.VersionInfoKey;

        return addSchema(schemaInfo, etag, versionPredicate);
    }

    public CompletableFuture<VersionInfo> addSchemaToSubGroup(String subgroup, SchemaInfo schemaInfo, Position etag) {
        Predicate<IndexRecord.IndexKey> versionForSubgroup = x -> x instanceof IndexRecord.VersionInfoKey &&
                ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(subgroup);

        return addSchema(schemaInfo, etag, versionForSubgroup);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<Position> updateValidationPolicy(SchemaValidationRules policy, Position etag) {
        return writeToLog(new Record.ValidationRecord(policy), etag)
                .thenCompose(logPos -> {
                    Operation.GetAndSet getAndSet = new Operation.GetAndSet(new IndexRecord.ValidationPolicyKey(), new IndexRecord.WALPositionValue(logPos),
                            x -> logPos.getPosition().compareTo(((IndexRecord.WALPositionValue) x).getPosition().getPosition()) > 0);
                    return updateIndex(Collections.singletonList(getAndSet)).thenApply(v -> logPos);
                });
    }

    public CompletableFuture<GroupProperties> getGroupProperties() {
        CompletableFuture<Record.GroupPropertiesRecord> grpPropertiesFuture = getGroupPropertiesRecord();

        CompletableFuture<SchemaValidationRules> rulesFuture = getCurrentValidationRules();

        return CompletableFuture.allOf(grpPropertiesFuture, rulesFuture)
                                .thenApply(v -> {
                                    Record.GroupPropertiesRecord properties = grpPropertiesFuture.join();
                                    SchemaValidationRules rules = rulesFuture.join();
                                    return new GroupProperties(properties.getSchemaType(), rules,
                                            properties.isSubgroupBySchemaName(), properties.isEnableEncoding());
                                });
    }

    private CompletableFuture<Void> updateIndex(List<Operation> operations) {
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        operations.forEach(operation -> {
            if (operation instanceof Operation.Add) {
                futures.add(index.addEntry(((Operation.Add) operation).getKey(), ((Operation.Add) operation).getValue()));
            } else if (operation instanceof Operation.GetAndSet) {
                Operation.GetAndSet op = (Operation.GetAndSet) operation;
                futures.add(Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .runAsync(() -> index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class)
                                      .thenCompose(existing -> {
                                 if (existing == null) {
                                     return index.addEntry(op.getKey(), op.getValue());
                                 } else if (op.getCondition().test(existing.getValue())) {
                                     return index.updateEntry(op.getKey(), op.getValue(), existing.getVersion());
                                 } else {
                                     return CompletableFuture.completedFuture(null);
                                 }
                             }), executor));
            } else if (operation instanceof Operation.AddToList) {
                Operation.AddToList op = (Operation.AddToList) operation;
                futures.add(Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .runAsync(() -> index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class)
                                      .thenCompose(existing -> {
                              if (existing == null) {
                                  return index.addEntry(op.getKey(), op.getValue());
                              } else if (op.getValue() instanceof IndexRecord.SchemaVersionValue) {
                                      IndexRecord.SchemaVersionValue existingList = (IndexRecord.SchemaVersionValue) existing.getValue();
                                      IndexRecord.SchemaVersionValue toAdd = (IndexRecord.SchemaVersionValue) op.getValue();
                                      Set<VersionInfo> set = new HashSet<>(existingList.getVersions());
                                      set.addAll(toAdd.getVersions());

                                      IndexRecord.SchemaVersionValue newValue = new IndexRecord.SchemaVersionValue(new ArrayList<>(set));
                                      return index.updateEntry(op.getKey(), newValue, existing.getVersion());
                              } else {
                                  return CompletableFuture.completedFuture(null);
                              }
                          }), executor));
            }
        });
        
        return Futures.allOf(futures);
    }

    private CompletableFuture<VersionInfo> addSchema(SchemaInfo schemaInfo, Position etag, Predicate<IndexRecord.IndexKey> versionPredicate) {
        return getNextVersion(versionPredicate)
                .thenCompose(nextVersion -> {
                    VersionInfo next = new VersionInfo(schemaInfo.getName(), nextVersion);
                    return writeToLog(new Record.SchemaRecord(schemaInfo, next), etag)
                            .thenCompose(logPos -> {
                                Operation.Add add = new Operation.Add(new IndexRecord.VersionInfoKey(next), new IndexRecord.WALPositionValue(logPos));
                                Operation.AddToList addToList = new Operation.AddToList(new IndexRecord.SchemaInfoKey(getFingerprint(schemaInfo)),
                                        new IndexRecord.SchemaVersionValue(Collections.singletonList(next)));
                                return updateIndex(Lists.newArrayList(add, addToList)).thenApply(v -> next);
                            });
                });
    }

    private CompletableFuture<Integer> getNextVersion(Predicate<IndexRecord.IndexKey> versionPredicate) {
        return getLatestEntryFor(versionPredicate, VERSION_COMPARATOR)
                .thenApply(entry -> {
                    if (entry == null) {
                        return 0;
                    } else {
                        return ((IndexRecord.VersionInfoKey) entry.getKey()).getVersionInfo().getVersion() + 1;                        
                    }
                });
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HASH.hashBytes(schemaInfo.getSchemaData()).asLong();
    }

    private CompletableFuture<Index.Entry> getLatestEntryFor(Predicate<IndexRecord.IndexKey> predicate, Comparator<Index.Entry> entryComparator) {
        return sync()
                .thenCompose(v -> index.getAllEntries(predicate)
                                       .thenApply(entries -> entries.stream().max(entryComparator).orElse(null)));
    }

    private CompletableFuture<EncodingId> getNextEncodingId() {
        Predicate<IndexRecord.IndexKey> predicate = x -> x instanceof IndexRecord.EncodingIdIndex;
        return getLatestEntryFor(predicate, ENCODING_ID_COMPARATOR)
                .thenApply(entry -> {
                    if (entry == null) {
                        return new EncodingId(0);
                    } else {
                        IndexRecord.EncodingIdIndex index = (IndexRecord.EncodingIdIndex) entry.getKey();
                        return new EncodingId(index.getEncodingId().getId() + 1);
                    }
                });
    }
    
    private CompletableFuture<Record.GroupPropertiesRecord> getGroupPropertiesRecord() {
        return index.getRecord(GROUP_PROPERTY_INDEX_KEY, IndexRecord.WALPositionValue.class)
             .thenCompose(record -> {
                 if (record == null) {
                     return sync().thenCompose(v ->
                             index.getRecord(GROUP_PROPERTY_INDEX_KEY, IndexRecord.WALPositionValue.class));
                 } else {
                     return CompletableFuture.completedFuture(record);
                 }
             }).thenCompose(record -> wal.readAt(record.getPosition(), Record.GroupPropertiesRecord.class));
    }
}