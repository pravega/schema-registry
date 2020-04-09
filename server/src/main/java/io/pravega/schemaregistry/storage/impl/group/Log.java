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
     * Identifies head of the log.  
     * @return Position identifying the head of the log. 
     */
    CompletableFuture<Position> getFirstEtag();
    
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

    /**
     * Method to read at a specific location in the log. 
     * 
     * @param position position to read at. 
     * @param tClass Class of entity to read. 
     * @param <T> Type of entity to read. 
     * @return Record of type T read back from the position in the log. 
     */
    <T extends Record> CompletableFuture<T> readAt(Position position, Class<T> tClass);

    /**
     * Method to read all records from the specified position. 
     * 
     * @param position position to read from. 
     * @return A list of records with their positions. 
     */
    CompletableFuture<List<RecordWithPosition>> readFrom(Position position);
}
