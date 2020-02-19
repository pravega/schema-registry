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

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.contract.data.CompressionType;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.storage.Position;
import lombok.Data;

import java.util.List;
import java.util.Map;

public interface IndexRecord {
    Map<Class<? extends IndexKey>, Class<? extends IndexValue>> ACCEPTED_KEY_VALUES =
            ImmutableMap.<Class<? extends IndexKey>, Class<? extends IndexValue>>builder()
                    .put(VersionInfoKey.class, WALPositionValue.class)
                    .put(SchemaInfoKey.class, SchemaVersionValue.class)
                    .put(ValidationPolicyKey.class, WALPositionValue.class)
                    .put(SyncdTillKey.class, WALPositionValue.class)
                    .put(EncodingIdIndex.class, EncodingInfoIndex.class)
                    .put(EncodingInfoIndex.class, EncodingIdIndex.class)
                    .build();

    interface IndexKey {
        
    }
    
    interface IndexValue {
        
    }

    @Data
    class WALPositionValue implements IndexValue {
        private final Position position;
    }

    @Data
    class SchemaInfoKey implements IndexKey {
        private final long fingerprint;
    }

    @Data
    class VersionInfoKey implements IndexKey {
        private final VersionInfo versionInfo;
    }

    @Data
    class SchemaVersionValue implements IndexValue {
        private final List<VersionInfo> versions;
    }

    @Data
    class EncodingInfoIndex implements IndexKey, IndexValue {
        private final VersionInfo versionInfo;
        private final CompressionType compressionType;
    }
    
    @Data
    class EncodingIdIndex implements IndexKey, IndexValue {
        private final EncodingId encodingId;
    }

    @Data
    class ValidationPolicyKey implements IndexKey {
    }
    
    @Data
    class SyncdTillKey implements IndexKey {
    }
}
