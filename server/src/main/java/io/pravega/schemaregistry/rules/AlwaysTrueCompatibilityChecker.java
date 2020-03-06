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

import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.util.List;

/**
 * Always true implementation of Compatibility Checker that returns true for each of the checks. 
 */
public class AlwaysTrueCompatibilityChecker implements CompatibilityChecker {
    @Override
    public boolean canRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        return true;
    }

    @Override
    public boolean canBeRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        return true;
    }

    @Override
    public boolean canMutuallyRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst) {
        return true;
    }
}
