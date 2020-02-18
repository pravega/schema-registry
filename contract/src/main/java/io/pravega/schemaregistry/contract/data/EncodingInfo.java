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
 * Encoding Info describes the details of encoding for each event payload. Each combination of schema version and compression type
 * is uniquely identified by an {@link EncodingId}. 
 * The registry service exposes APIs to generate or resolve {@link EncodingId} to {@link EncodingInfo}.
 */
@Data
public class EncodingInfo {
    private final VersionInfo versionInfo;
    private final SchemaInfo schemaInfo;
    private final CompressionType compression;
}
