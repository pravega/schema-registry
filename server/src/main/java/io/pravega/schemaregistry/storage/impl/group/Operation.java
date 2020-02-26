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

import io.pravega.schemaregistry.storage.records.IndexRecord;
import lombok.Data;

import java.util.function.Predicate;

interface Operation {
    class Noop implements Operation {
    }

    @Data
    class Add implements Operation {
        private final IndexRecord.IndexKey key;
        private final IndexRecord.IndexValue value;
    }

    @Data
    class AddToList implements Operation {
        private final IndexRecord.IndexKey key;
        private final IndexRecord.IndexValue value;
    }

    @Data
    class GetAndSet implements Operation {
        private final IndexRecord.IndexKey key;
        private final IndexRecord.IndexValue value;
        private final Predicate<IndexRecord.IndexValue> condition;
    }
}
