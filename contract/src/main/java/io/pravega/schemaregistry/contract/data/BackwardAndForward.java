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

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Defines different BackwardAndForward policy options for schema evolution for schemas within a group.
 * The choice of compatibility policy tells the Schema Registry service whether a schema should be accepted to evolve
 * into new schema by comparing it with one or more existing versions of the schema. 
 *
 * {@link Backward}: a new schema can be used to read data written by previous schema. 
 * {@link BackwardTransitive}: a new schema can be used read data written by any of previous schemas. 
 * {@link BackwardTill}: a new schema can be used to read data written by any of previous schemas till specified schema. 
 * {@link Forward}: previous schema can be used to read data written by new schema. 
 * {@link ForwardTransitive}: all previous schemas can read data written by new schema. 
 * {@link ForwardTill}: All previous schema versions till specified schema version can read data written by new schema. 
 */
@Data
@Builder
public class BackwardAndForward {

    private final BackwardPolicy backwardPolicy;
    private final ForwardPolicy forwardPolicy;

    BackwardAndForward(BackwardPolicy backwardPolicy, ForwardPolicy forwardPolicy) {
        Preconditions.checkArgument(backwardPolicy != null || forwardPolicy != null);
        Preconditions.checkArgument(backwardPolicy == null || backwardPolicy instanceof Backward
                || backwardPolicy instanceof BackwardTill || backwardPolicy instanceof BackwardTransitive);
        Preconditions.checkArgument(forwardPolicy == null || forwardPolicy instanceof Forward
                || forwardPolicy instanceof ForwardTill || forwardPolicy instanceof ForwardTransitive);
        this.backwardPolicy = backwardPolicy;
        this.forwardPolicy = forwardPolicy;
    }

    public interface BackwardPolicy {
    }

    public interface ForwardPolicy {
    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Backward implements BackwardPolicy {
        public static class BackwardBuilder implements ObjectBuilder<Backward> {
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class BackwardTill implements BackwardPolicy {
        private final VersionInfo versionInfo;

        public static class BackwardTillBuilder implements ObjectBuilder<BackwardTill> {
        }
    }

    @Builder
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class BackwardTransitive implements BackwardPolicy {
        public static class BackwardTransitiveBuilder implements ObjectBuilder<BackwardTransitive> {
        }
    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Forward implements ForwardPolicy {
        public static class ForwardBuilder implements ObjectBuilder<Forward> {
        }
    }

    @Data
    @Builder
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class ForwardTill implements ForwardPolicy {
        private final VersionInfo versionInfo;
        public static class ForwardTillBuilder implements ObjectBuilder<ForwardTill> {
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Builder
    public static class ForwardTransitive implements ForwardPolicy {
        public static class ForwardTransitiveBuilder implements ObjectBuilder<ForwardTransitive> {
        }
    }

    public static class BackwardAndForwardBuilder implements ObjectBuilder<BackwardAndForward> {
    }
}
