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
 * Describes changes to the group and the validation rules {@link GroupHistoryRecord#rules} that were 
 * applied while registering {@link GroupHistoryRecord#schema} and the unique {@link GroupHistoryRecord#version} identifier 
 * that was assigned to it. 
 * It also has {@link GroupHistoryRecord#timestamp} when the schema was added and includes an optional 
 * {@link GroupHistoryRecord#schemaString} which is populated only if serialization format is one of {@link SerializationFormat#Avro}
 * {@link SerializationFormat#Json} or {@link SerializationFormat#Protobuf}. This string is just to help make the schema human readable. 
 */
@Data
public class GroupHistoryRecord {
    private final SchemaInfo schema;
    private final VersionInfo version;
    private final SchemaValidationRules rules;
    private final long timestamp;
    private final String schemaString;
}


