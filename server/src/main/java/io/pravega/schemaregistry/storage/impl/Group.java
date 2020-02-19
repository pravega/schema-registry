/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.schemaregistry.ListWithToken;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
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
import io.pravega.schemaregistry.storage.records.RecordWithPosition;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Group {
    private static final IndexRecord.SyncdTillKey SYNCD_TILL = new IndexRecord.SyncdTillKey();
    private static final IndexRecord.ValidationPolicyKey VALIDATION_POLICY_INDEX = new IndexRecord.ValidationPolicyKey();
    private static final Comparator<KeyValue.Entry> VERSION_COMPARATOR = (v1, v2) -> {
        IndexRecord.VersionInfoKey version1 = (IndexRecord.VersionInfoKey) v1.getKey();
        IndexRecord.VersionInfoKey version2 = (IndexRecord.VersionInfoKey) v2.getKey();
        return Integer.compare(version1.getVersionInfo().getVersion(), version2.getVersionInfo().getVersion());
    };
    private static final Comparator<KeyValue.Entry> ENCODING_ID_COMPARATOR = (v1, v2) -> {
        IndexRecord.EncodingIdIndex id1 = (IndexRecord.EncodingIdIndex) v1.getKey();
        IndexRecord.EncodingIdIndex id2 = (IndexRecord.EncodingIdIndex) v2.getKey();
        return Integer.compare(id1.getEncodingId().getId(), id2.getEncodingId().getId());
    };
    private static final HashFunction HASH = Hashing.murmur3_128();

    private final Log wal;

    private final KeyValue index;
    @Getter
    private final SchemaType schemaType;
    @Getter
    private final boolean subgroupByEventType;
    @Getter
    private final boolean enableEncoding;

    public Group(SchemaType schemaType, boolean subgroupByEventType, boolean enableEncoding,
                 SchemaValidationRules schemaValidationRules, Log wal, KeyValue index) {
        this.schemaType = schemaType;
        this.subgroupByEventType = subgroupByEventType;
        this.enableEncoding = enableEncoding;
        this.wal = wal;
        this.index = index;
        Position etag = this.wal.writeToLog(new Record.ValidationRecord(schemaValidationRules), null);

        IndexRecord.WALPositionValue walPosition = new IndexRecord.WALPositionValue(etag);
        Operation.Add addPolicy = new Operation.Add(VALIDATION_POLICY_INDEX, walPosition);
        Operation.Add addSyncTill = new Operation.Add(SYNCD_TILL, walPosition);
        updateIndex(Lists.newArrayList(addPolicy, addSyncTill));
    }

    public Position sync() {
        KeyValue.Value<IndexRecord.WALPositionValue> value = index.getRecordWithVersion(SYNCD_TILL,
                IndexRecord.WALPositionValue.class);
        IndexRecord.WALPositionValue syncdTill = value.getValue();
        List<RecordWithPosition> list = wal.readFrom((Position) syncdTill.getPosition());
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
                        m -> ((IndexRecord.WALPositionValue) m).getPosition().getPosition() < x.getPosition().getPosition());
                operations.add(getAndSet);
            }
        });
        updateIndex(operations);
        return (Position) list.get(list.size() - 1).getPosition();
    }

    public SchemaValidationRules getCurrentValidationRules() {
        sync();
        IndexRecord.WALPositionValue validationRecordPosition = index.getRecord(VALIDATION_POLICY_INDEX, IndexRecord.WALPositionValue.class);
        return wal.readAt((Position) validationRecordPosition.getPosition(), Record.ValidationRecord.class).getValidationRules();
    }

    public Position getCurrentEtag() {
        return wal.getCurrentEtag();
    }

    public Position writeToLog(Record record, Position etag) {
        return wal.writeToLog(record, etag);
    }

    public ListWithToken<String> getSubgroups() {
        if (!isSubgroupByEventType()) {
            throw new IllegalStateException();
        }

        // get all index keys
        List<String> list = index.getAllKeys().stream().filter(x -> x instanceof IndexRecord.VersionInfoKey).map(x -> ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName())
                                 .distinct().collect(Collectors.toList());
        return new ListWithToken<>(list, null);
    }

    public ListWithToken<SchemaWithVersion> getSchemas() {
        List<RecordWithPosition> records = wal.readFrom(null);
        List<SchemaWithVersion> schemas = records.stream().filter(x -> x.getRecord() instanceof Record.SchemaRecord).map(x -> {
            Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
            return new SchemaWithVersion(record.getSchemaInfo(), record.getVersionInfo());
        }).collect(Collectors.toList());

        return new ListWithToken<>(schemas, null);
    }

    public ListWithToken<SchemaWithVersion> getSchemas(String subgroup) {
        List<RecordWithPosition> records = wal.readFrom(null);
        List<SchemaWithVersion> schemas = records.stream().filter(x -> x.getRecord() instanceof Record.SchemaRecord
                && ((Record.SchemaRecord) x.getRecord()).getSchemaInfo().getName().equals(subgroup)).map(x -> {
            Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
            return new SchemaWithVersion(record.getSchemaInfo(), record.getVersionInfo());
        }).collect(Collectors.toList());

        return new ListWithToken<>(schemas, null);
    }

    public SchemaInfo getSchema(VersionInfo versionInfo) {
        IndexRecord.WALPositionValue record = index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class);
        if (record == null) {
            sync();
            record = index.getRecord(new IndexRecord.VersionInfoKey(versionInfo), IndexRecord.WALPositionValue.class);
        }
        return wal.readAt((Position) record.getPosition(), Record.SchemaRecord.class).getSchemaInfo();
    }

    public VersionInfo getVersion(SchemaInfo schemaInfo) {
        long fingerPrint = getFingerprint(schemaInfo);
        IndexRecord.SchemaInfoKey key = new IndexRecord.SchemaInfoKey(fingerPrint);

        List<VersionInfo> checked = new LinkedList<>();
        VersionInfo found;
        IndexRecord.SchemaVersionValue record = index.getRecord(key, IndexRecord.SchemaVersionValue.class);
        if (record != null) {
            found = findVersion(record.getVersions(), schemaInfo);
            if (found != null) {
                return found;
            }
            checked.addAll(record.getVersions());
        }

        // record not found. sync and check again
        sync();
        record = index.getRecord(key, IndexRecord.SchemaVersionValue.class);
        if (record != null) {
            List<VersionInfo> unchecked = record.getVersions().stream().filter(x -> !checked.contains(x)).collect(Collectors.toList());
            found = findVersion(unchecked, schemaInfo);
            if (found != null) {
                return found;
            }
        }

        // still not found. so throw exception
        throw new StoreExceptions.DataNotFoundException();
    }

    public VersionInfo findVersion(List<VersionInfo> versions, SchemaInfo toFind) {
        for (VersionInfo version : versions) {
            SchemaInfo schema = getSchema(version);
            if (Arrays.equals(schema.getSchemaData(), toFind.getSchemaData())) {
                return version;
            }
        }
        return null;
    }

    public EncodingId getOrCreateEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        EncodingId encodingId = getEncodingId(versionInfo, compressionType);
        Position position;

        if (encodingId == null) {
            position = sync();
            encodingId = getEncodingId(versionInfo, compressionType);
        } else {
            return encodingId;
        }

        if (encodingId == null) {
            EncodingId id = getNextEncodingId();
            writeToLog(new Record.EncodingRecord(id, versionInfo, compressionType), position);
            IndexRecord.EncodingIdIndex idIndex = new IndexRecord.EncodingIdIndex(id);
            IndexRecord.EncodingInfoIndex infoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, compressionType);
            Operation.Add idToInfo = new Operation.Add(idIndex, infoIndex);
            Operation.Add infoToId = new Operation.Add(infoIndex, idIndex);
            updateIndex(Lists.newArrayList(idToInfo, infoToId));

            return id;
        } else {
            return encodingId;
        }
    }

    public EncodingId getEncodingId(VersionInfo versionInfo, CompressionType compressionType) {
        IndexRecord.EncodingInfoIndex encodingInfoIndex = new IndexRecord.EncodingInfoIndex(versionInfo, compressionType);
        IndexRecord.EncodingIdIndex record = index.getRecord(encodingInfoIndex, IndexRecord.EncodingIdIndex.class);
        return record == null ? null : record.getEncodingId();
    }

    public EncodingInfo getEncodingInfo(EncodingId encodingId) {
        IndexRecord.EncodingIdIndex encodingIdIndex = new IndexRecord.EncodingIdIndex(encodingId);
        IndexRecord.EncodingInfoIndex encodingInfo = index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class);
        if (encodingInfo == null) {
            sync();
            encodingInfo = index.getRecord(encodingIdIndex, IndexRecord.EncodingInfoIndex.class);
            if (encodingInfo == null) {
                throw new StoreExceptions.DataNotFoundException();
            }
        }

        SchemaInfo schemaInfo = getSchema(encodingInfo.getVersionInfo());
        return new EncodingInfo(encodingInfo.getVersionInfo(), schemaInfo, encodingInfo.getCompressionType());
    }

    public SchemaWithVersion getLatestSchema() {
        Predicate<IndexRecord.IndexKey> versionPredicate = x -> x instanceof IndexRecord.VersionInfoKey;

        Optional<KeyValue.Entry> max = getLatestEntryFor(versionPredicate, VERSION_COMPARATOR);

        return max.map(x -> {
            IndexRecord.VersionInfoKey key = (IndexRecord.VersionInfoKey) x.getKey();
            VersionInfo version = key.getVersionInfo();
            SchemaInfo schema = getSchema(version);
            return new SchemaWithVersion(schema, version);
        }).orElse(null);
    }

    public SchemaWithVersion getLatestSchema(String subgroup) {
        Predicate<IndexRecord.IndexKey> versionForSubgroup = x -> x instanceof IndexRecord.VersionInfoKey &&
                ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(subgroup);

        Optional<KeyValue.Entry> max = getLatestEntryFor(versionForSubgroup, VERSION_COMPARATOR);
        return max.map(x -> {
            IndexRecord.VersionInfoKey key = (IndexRecord.VersionInfoKey) x.getKey();
            VersionInfo version = key.getVersionInfo();
            SchemaInfo schema = getSchema(version);
            return new SchemaWithVersion(schema, version);
        }).orElse(null);
    }

    public List<CompressionType> getCompressions() {
        sync();
        Predicate<IndexRecord.IndexKey> encodingInfoPredicate = x -> x instanceof IndexRecord.EncodingInfoIndex;
        return index.getAllEntries(encodingInfoPredicate)
                    .stream().map(x -> {
                    IndexRecord.EncodingInfoIndex encodingInfoIndex = (IndexRecord.EncodingInfoIndex) x.getKey();
                    return encodingInfoIndex.getCompressionType();
                }).distinct().collect(Collectors.toList());
    }

    public List<SchemaEvolutionEpoch> getHistory() {
        AtomicReference<SchemaValidationRules> rulesRef = new AtomicReference<>();
        List<SchemaEvolutionEpoch> epochs = new LinkedList<>();
        wal.readFrom(null).forEach(x -> {
            if (x.getRecord() instanceof Record.SchemaRecord) {
                Record.SchemaRecord record = (Record.SchemaRecord) x.getRecord();
                SchemaEvolutionEpoch epoch = new SchemaEvolutionEpoch(record.getSchemaInfo(), record.getVersionInfo(),
                        rulesRef.get());
                epochs.add(epoch);
            } else if (x.getRecord() instanceof Record.ValidationRecord) {
                rulesRef.set(((Record.ValidationRecord) x.getRecord()).getValidationRules());
            }
        });
        return epochs;
    }

    public List<SchemaEvolutionEpoch> getHistory(String subgroup) {
        return getHistory().stream().filter(x -> x.getSchema().getName().equals(subgroup)).collect(Collectors.toList());

    }

    public VersionInfo addSchemaToGroup(SchemaInfo schemaInfo, Position etag) {
        Predicate<IndexRecord.IndexKey> versionPredicate = x -> x instanceof IndexRecord.VersionInfoKey;

        return addSchema(schemaInfo, (Position) etag, versionPredicate);
    }

    public VersionInfo addSchemaToSubGroup(String subgroup, SchemaInfo schemaInfo, Position etag) {
        Predicate<IndexRecord.IndexKey> versionForSubgroup = x -> x instanceof IndexRecord.VersionInfoKey &&
                ((IndexRecord.VersionInfoKey) x).getVersionInfo().getSchemaName().equals(subgroup);

        return addSchema(schemaInfo, (Position) etag, versionForSubgroup);
    }

    public Position updateValidationPolicy(SchemaValidationRules policy, Position etag) {
        Position logPos = writeToLog(new Record.ValidationRecord(policy), (Position) etag);
        Operation.GetAndSet getAndSet = new Operation.GetAndSet(new IndexRecord.ValidationPolicyKey(), new IndexRecord.WALPositionValue(logPos),
                x -> logPos.getPosition() > ((IndexRecord.WALPositionValue) x).getPosition().getPosition());
        updateIndex(Collections.singletonList(getAndSet));
        return logPos;
    }

    private void updateIndex(List<Operation> operations) {
        operations.forEach(operation -> {
            if (operation instanceof Operation.Add) {
                index.addEntry(((Operation.Add) operation).getKey(), ((Operation.Add) operation).getValue());
            } else if (operation instanceof Operation.GetAndSet) {
                Operation.GetAndSet op = (Operation.GetAndSet) operation;
                Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .run(() -> {
                         KeyValue.Value existing = index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class);
                         if (existing == null || op.getCondition().test(existing.getValue())) {
                             index.updateEntry(op.getKey(), op.getValue(), existing == null ? 0 : existing.getVersion());
                         }
                         return null;
                     });
            } else if (operation instanceof Operation.AddToList) {
                Operation.AddToList op = (Operation.AddToList) operation;
                Retry.withExpBackoff(1, 2, Integer.MAX_VALUE, 100)
                     .retryWhen(x -> Exceptions.unwrap(x) instanceof StoreExceptions.WriteConflictException)
                     .run(() -> {
                         KeyValue.Value existing = index.getRecordWithVersion(op.getKey(), IndexRecord.IndexValue.class);
                         if (existing == null) {
                             index.updateEntry(op.getKey(), op.getValue(), 0);
                         } else {
                             if (op.getValue() instanceof IndexRecord.SchemaVersionValue) {
                                 IndexRecord.SchemaVersionValue existingList = (IndexRecord.SchemaVersionValue) existing.getValue();
                                 IndexRecord.SchemaVersionValue toAdd = (IndexRecord.SchemaVersionValue) op.getValue();
                                 Set<VersionInfo> set = new HashSet<>(existingList.getVersions());
                                 set.addAll(toAdd.getVersions());

                                 IndexRecord.SchemaVersionValue newValue = new IndexRecord.SchemaVersionValue(new ArrayList<>(set));
                                 index.updateEntry(op.getKey(), newValue, existing.getVersion());
                             }
                         }
                         return null;
                     });
            }
        });
    }

    private VersionInfo addSchema(SchemaInfo schemaInfo, Position etag, Predicate<IndexRecord.IndexKey> versionPredicate) {
        int nextVersion = getNextVersion(versionPredicate);
        VersionInfo next = new VersionInfo(schemaInfo.getName(), nextVersion);
        Position logPos = writeToLog(new Record.SchemaRecord(schemaInfo, next), etag);
        Operation.Add add = new Operation.Add(new IndexRecord.VersionInfoKey(next), new IndexRecord.WALPositionValue(logPos));
        Operation.AddToList addToList = new Operation.AddToList(new IndexRecord.SchemaInfoKey(getFingerprint(schemaInfo)),
                new IndexRecord.SchemaVersionValue(Collections.singletonList(next)));
        updateIndex(Lists.newArrayList(add, addToList));
        return next;
    }

    private int getNextVersion(Predicate<IndexRecord.IndexKey> versionPredicate) {
        VersionInfo previous = getLatestEntryFor(versionPredicate, VERSION_COMPARATOR)
                .map(x -> ((IndexRecord.VersionInfoKey) x.getKey()).getVersionInfo()).orElse(null);

        return previous == null ? 0 : previous.getVersion() + 1;
    }

    private long getFingerprint(SchemaInfo schemaInfo) {
        return HASH.hashBytes(schemaInfo.getSchemaData()).asLong();
    }

    private Optional<KeyValue.Entry> getLatestEntryFor(Predicate<IndexRecord.IndexKey> predicate, Comparator<KeyValue.Entry> entryComparator) {
        sync();
        return index.getAllEntries(predicate).stream().max(entryComparator);
    }

    private EncodingId getNextEncodingId() {
        Predicate<IndexRecord.IndexKey> predicate = x -> x instanceof IndexRecord.EncodingIdIndex;
        Optional<KeyValue.Entry> max = getLatestEntryFor(predicate, ENCODING_ID_COMPARATOR);
        return max.map(x -> {
            IndexRecord.EncodingIdIndex index = (IndexRecord.EncodingIdIndex) x.getKey();
            return new EncodingId(index.getEncodingId().getId() + 1);
        }).orElse(new EncodingId(0));
    }
}