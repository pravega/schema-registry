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

import io.pravega.schemaregistry.storage.Position;
import io.pravega.schemaregistry.storage.records.Record;
import io.pravega.schemaregistry.storage.records.RecordWithPosition;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Append only write ahead log where the {@link Group}'s state is stored. 
 */
public interface Log {
    /**
     * Identifies current tail of the log where conditional writes could happen.  
     * @return Position identifying the tail of the log. 
     */
    CompletableFuture<Position> getCurrentEtag();

    /**
     * Method to conditionally write to the log at the supplied position. 
     * 
     * @param record record to write. 
     * @param etag position where the record should be written. If supplied position is null, its written at the head of the log.  
     * @return Position where the record was written. Typically it will be same as the input position, but if input position is null, 
     * this identifies the head position of the log
     */
    CompletableFuture<Position> writeToLog(Record record, Position etag);

    <T extends Record> CompletableFuture<T> readAt(Position position, Class<T> tClass);

    CompletableFuture<List<RecordWithPosition>> readFrom(Position position);
}
