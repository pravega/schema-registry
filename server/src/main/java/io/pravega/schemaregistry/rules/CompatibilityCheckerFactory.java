/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.rules;

import io.pravega.schemaregistry.contract.data.SchemaType;

public class CompatibilityCheckerFactory {
    private static final AvroCompatibilityChecker AVRO_COMPATIBILITY_CHECKER = new AvroCompatibilityChecker();
    private static final AlwaysTrueCompatibilityChecker ALWAYS_TRUE_COMPATIBILITY_CHECKER = new AlwaysTrueCompatibilityChecker();
    
    public static CompatibilityChecker getCompatibilityChecker(SchemaType schemaType) {
        if (schemaType.getSchemaType().equals(SchemaType.Type.Avro)) {
            return AVRO_COMPATIBILITY_CHECKER;
        } else {
            return ALWAYS_TRUE_COMPATIBILITY_CHECKER;
        }
    }
}
