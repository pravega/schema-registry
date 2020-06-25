/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import lombok.Data;

/**
 * Describes changes to the group and the compatibility {@link GroupHistoryRecord#compatibility} that were 
 * applied while registering {@link GroupHistoryRecord#schemaInfo} and the unique {@link GroupHistoryRecord#versionInfo} identifier 
 * that was assigned to it. 
 * It also has {@link GroupHistoryRecord#timestamp} when the schemaInfo was added and includes an optional 
 * {@link GroupHistoryRecord#schemaString} which is populated only if serialization format is one of {@link SerializationFormat#Avro}
 * {@link SerializationFormat#Json} or {@link SerializationFormat#Protobuf}. This string is just to help make the schemaInfo human readable. 
 */
@Data
public class GroupHistoryRecord {
    /**
     * Schema information object for the schemaInfo that was added to the group.
     */
    private final SchemaInfo schemaInfo;
    /**
     * Version information object that uniquely identifies the schemaInfo in the group. 
     */
    private final VersionInfo versionInfo;
    /**
     * Compatibility applied at the time when the schemaInfo was registered. 
     */
    private final Compatibility compatibility;
    /**
     * Service's Time when the schemaInfo was registered. 
     */
    private final long timestamp;
    /**
     * A json format string representing the schemaInfo. This string will be populated only for serialization formats 
     * that the service can parse. 
     */
    private final String schemaString;
}


