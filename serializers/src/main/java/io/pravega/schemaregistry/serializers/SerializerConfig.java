/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.serializers;

import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SerializerConfig {
    private final SchemaRegistryContract.SchemaType schemaType;
    private boolean encodeHeader;
    private final boolean automaticallyRegisterSchema;
    private final boolean deserializeIntoWriterSchema;
    private final SchemaRegistryContract.CompressionType compressionType;
    private final SchemaRegistryContract.SchemaValidationRules validationRules;
}
