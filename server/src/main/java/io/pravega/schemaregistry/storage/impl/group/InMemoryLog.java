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

import io.pravega.common.concurrent.Futures;
import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.StoreExceptions;
import io.pravega.schemaregistry.storage.records.InMemoryPosition;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InMemoryLog implements Log {
    private static final InMemoryPosition NULL_POSITION = new InMemoryPosition(-1);
    private static final InMemoryPosition HEAD_POSITION = new InMemoryPosition(0);
    
    @GuardedBy("$lock")
    private final List<Record> log = new LinkedList<>();
    
    @Override
    @Synchronized
    public CompletableFuture<Position> getCurrentEtag() {
        return CompletableFuture.completedFuture(new InMemoryPosition(log.size() - 1));
    }

    @Override
    @Synchronized
    public CompletableFuture<Position> writeToLog(Record record, Position position) {
        InMemoryPosition pos = position == null ? NULL_POSITION : (InMemoryPosition) position;

        if (pos.getPosition() != log.size() - 1) {
            throw StoreExceptions.create(StoreExceptions.Type.WRITE_CONFLICT, record.getClass().toString());
        }

        log.add(record);
        return getCurrentEtag();
    }

    @Override
    @SuppressWarnings("unchecked")
    @Synchronized
    public <T extends Record> CompletableFuture<T> readAt(Position position, Class<T> tClass) {
        InMemoryPosition pos = position == null ? HEAD_POSITION : (InMemoryPosition) position;
        Record record = log.get(pos.getPosition().intValue());
        if (tClass.isAssignableFrom(record.getClass())) {
            return CompletableFuture.completedFuture((T) record);
        } else {
            return Futures.failedFuture(new IllegalArgumentException());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<List<RecordWithPosition>> readFrom(Position position) {
        InMemoryPosition pos = position == null ? HEAD_POSITION : (InMemoryPosition) position;

        int startingPos = pos.getPosition().intValue();
        List<RecordWithPosition> recordWithPositions = new ArrayList<>(log.size() - startingPos);
        for (int i = startingPos; i < log.size(); i++) {
            InMemoryPosition inMemoryPosition = new InMemoryPosition(i);
            recordWithPositions.add(new RecordWithPosition(inMemoryPosition, readAt(inMemoryPosition, Record.class).join()));
        }
        return CompletableFuture.completedFuture(recordWithPositions);
    }
}

