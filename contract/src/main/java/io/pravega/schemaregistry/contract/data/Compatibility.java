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
 * Defines different Compatibility policy options for schema evolution. 
 * Backward compatibility means a new schema can read data written by old schema. 
 * Forward compatibility means an old schema can read data written by new schema. 
 */
@Data
public class Compatibility implements SchemaValidationRule {
    private final CompatibilityType compatibility;
    private final VersionInfo backwardTill;
    private final VersionInfo forwardTill;

    public Compatibility(CompatibilityType compatibility) {
        this(compatibility, null, null);
    }

    public Compatibility(CompatibilityType compatibility, VersionInfo backwardTill, VersionInfo forwardTill) {
        this.compatibility = compatibility;
        this.backwardTill = backwardTill;
        this.forwardTill = forwardTill;
    }
    
    public static enum CompatibilityType {
        AllowAny, 
        DisallowAll, 
        Backward,
        BackwardTill,
        BackwardTransitive, 
        Forward, 
        ForwardTill,
        ForwardTransitive,
        BackwardTillAndForwardTill;
    }
}
