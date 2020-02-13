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

public interface Compatibility {

    public class AllowAny implements Compatibility {
    }

    public class DisallowAll implements Compatibility {
    }

    public class FullTransitive extends BackwardAndForward {
        public FullTransitive() {
            super(new Transitive(), new Transitive());
        }
    }

    @Data
    public class BackwardAndForward implements Compatibility {
        private final CompatibilityTill backwardCompatibilityRule;
        private final CompatibilityTill forwardCompatibilityRule;
    }

    @Data
    public class BackwardTill implements Compatibility {
        private final CompatibilityTill backwardCompatibilityRule;

        public BackwardTill(CompatibilityTill backwardCompatibilityRule) {
            this.backwardCompatibilityRule = backwardCompatibilityRule;
        }
    }

    @Data
    public class ForwardTill implements Compatibility {
        private final CompatibilityTill forwardCompatibilityRule;
    }

    public class BackwardTransitive extends BackwardTill {
        public BackwardTransitive() {
            super(new Transitive());
        }
    }

    public class ForwardTransitive extends BackwardTill {
        public ForwardTransitive() {
            super(new Transitive());
        }
    }

    @Data
    public class CompatibilityTill {
        private final SchemaRegistryContract.VersionInfo compatibilityTill;
    }

    public class Transitive extends CompatibilityTill {
        public Transitive() {
            super(SchemaRegistryContract.VersionInfo.NON_EXISTENT);
        }
    }
}
