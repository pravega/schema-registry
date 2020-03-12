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
 * Compatibility checker interface to compare a schema against existing schemas for compatibility. 
 */
public interface CompatibilityChecker {
    /**
     * Checks if toValidate can be used to read data written using all schemas in toValidateAgainst.
     * 
     * @param toValidate Reader schema.     
     * @param toValidateAgainst Writer schema. 
     * @return True if toValidate can be used to read data written using toValidate, false otherwise. 
     */
    boolean canRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst);

    /**
     * Checks if all schemas in toValidateAgainst can be used to read data written using toValidate.
     *
     * @param toValidate writer schema. 
     * @param toValidateAgainst reader schemas.
     * @return True if any of toValidateAgainst can be used to read data written using toValidate, false otherwise. 
     */
    boolean canBeRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst);

    /**
     * Checks if both toValidate and toValidateAgainst can be used to read data written with either. 
     *
     * @param toValidate Schema to check. 
     * @param toValidateAgainst All toValidateAgainst to check against.
     * @return True if toValidate can read and be readby all schemas in toValidateAgainst, false otherwise.  
     */
    boolean canMutuallyRead(SchemaInfo toValidate, List<SchemaInfo> toValidateAgainst);
}
