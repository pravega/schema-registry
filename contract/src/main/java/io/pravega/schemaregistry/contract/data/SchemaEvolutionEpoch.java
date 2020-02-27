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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Describes changes to the group (or subgroup) and the validation rules {@link SchemaEvolutionEpoch#rules} that were 
 * applied while registering {@link SchemaEvolutionEpoch#schema} and the unique {@link SchemaEvolutionEpoch#version} identifier 
 * that was assigned to it. 
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaEvolutionEpoch {
    public static final Serializer SERIALIZER = new Serializer();

    private final SchemaInfo schema;
    private final VersionInfo version;
    private final SchemaValidationRules rules;
    
    private static class SchemaEvolutionEpochBuilder implements ObjectBuilder<SchemaEvolutionEpoch> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<SchemaEvolutionEpoch, SchemaEvolutionEpoch.SchemaEvolutionEpochBuilder> {
        @Override
        protected SchemaEvolutionEpoch.SchemaEvolutionEpochBuilder newBuilder() {
            return SchemaEvolutionEpoch.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SchemaEvolutionEpoch e, RevisionDataOutput target) throws IOException {
            SchemaInfo.SERIALIZER.serialize(target, e.schema);
            VersionInfo.SERIALIZER.serialize(target, e.version);
            SchemaValidationRules.SERIALIZER.serialize(target, e.rules);
        }

        private void read00(RevisionDataInput source, SchemaEvolutionEpoch.SchemaEvolutionEpochBuilder b) throws IOException {
            b.schema(SchemaInfo.SERIALIZER.deserialize(source))
             .version(VersionInfo.SERIALIZER.deserialize(source))
             .rules(SchemaValidationRules.SERIALIZER.deserialize(source));
        }
    }
}

