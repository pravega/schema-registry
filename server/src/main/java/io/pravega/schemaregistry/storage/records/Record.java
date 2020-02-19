/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import lombok.Data;

public interface Record {
    @Data
    public class SchemaRecord implements Record {
        private final SchemaInfo schemaInfo;
        private final VersionInfo versionInfo;
    }
    
    @Data
    public class EncodingRecord implements Record {
        private final EncodingId encodingId;
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;
    }
    
    @Data
    public class ValidationRecord implements Record {
        private final SchemaValidationRules validationRules;
    }
}
