/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract;

import lombok.Data;

/**
 * Defines different Compatibility policy options for schema evolution. 
 * Backward compatibility means a new schema can read data written by old schema. 
 * Forward compatibility means an old schema can read data written by new schema. 
 */
public interface Compatibility {

    /**
     * Allow any changes to schemas. 
     */
    public class AllowAny implements Compatibility {
    }

    /**
     * Disallow all changes to schema. 
     */
    public class DisallowAll implements Compatibility {
    }

    /**
     * Check for compatibility with all previous schemas for both backward and forward compatibility. 
     */
    public class FullTransitive extends BackwardAndForward {
        public FullTransitive() {
            super(new Transitive(), new Transitive());
        }
    }

    /**
     * Check for compatibility with a subset of previous schemas for backward compatibility till version from  
     * {@linkplain CompatibilityTill#version} and forward compatibility till version from 
     * {@linkplain CompatibilityTill#version}.
     */
    @Data
    public class BackwardAndForward implements Compatibility {
        private final CompatibilityTill backwardCompatibilityRule;
        private final CompatibilityTill forwardCompatibilityRule;
    }

    /**
     * Check for compatibility with a subset of previous schemas for backward compatibility till version 
     * from {@linkplain CompatibilityTill#version}.  
     */
    @Data
    public class BackwardTill implements Compatibility {
        private final CompatibilityTill backwardCompatibilityRule;

        public BackwardTill(CompatibilityTill backwardCompatibilityRule) {
            this.backwardCompatibilityRule = backwardCompatibilityRule;
        }
    }

    /**
     * Check for compatibility with a subset of previous schemas for backward compatibility till version from 
     * {@linkplain CompatibilityTill#version}.
     */
    @Data
    public class ForwardTill implements Compatibility {
        private final CompatibilityTill forwardCompatibilityRule;
    }

    /**
     * Check for compatibility with all previous schemas for backward compatibility. 
     */
    public class BackwardTransitive extends BackwardTill {
        public BackwardTransitive() {
            super(new Transitive());
        }
    }

    /**
     * Check for compatibility with all previous schemas for forward compatibility. 
     */
    public class ForwardTransitive extends BackwardTill {
        public ForwardTransitive() {
            super(new Transitive());
        }
    }

    /**
     * Property on compatibility policy that identifies a previous schema version. 
     * It says compatibility check should be tested till specified version.   
     */
    @Data
    public class CompatibilityTill {
        private final SchemaRegistryContract.VersionInfo version;
    }

    /**
     * Property on compatibility policy that says compatibility checks should be tested with all previous schemas. 
     */
    public class Transitive extends CompatibilityTill {
        public Transitive() {
            super(SchemaRegistryContract.VersionInfo.NON_EXISTENT);
        }
    }
}
