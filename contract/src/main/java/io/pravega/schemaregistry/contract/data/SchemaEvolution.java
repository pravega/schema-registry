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
 * Describes changes to the group as schemas are registered. This contains {@link SchemaInfo} schema along with the 
 * validation rules {@link SchemaEvolution#rules} that were applied while registering schema. 
 * It also includes the unique {@link SchemaEvolution#version} identifier that was assigned to the schema. 
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaEvolution {
    public static final Serializer SERIALIZER = new Serializer();

    private final SchemaInfo schema;
    private final VersionInfo version;
    private final SchemaValidationRules rules;
    
    private static class SchemaEvolutionBuilder implements ObjectBuilder<SchemaEvolution> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<SchemaEvolution, SchemaEvolution.SchemaEvolutionBuilder> {
        @Override
        protected SchemaEvolution.SchemaEvolutionBuilder newBuilder() {
            return SchemaEvolution.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SchemaEvolution e, RevisionDataOutput target) throws IOException {
            SchemaInfo.SERIALIZER.serialize(target, e.schema);
            VersionInfo.SERIALIZER.serialize(target, e.version);
            SchemaValidationRules.SERIALIZER.serialize(target, e.rules);
        }

        private void read00(RevisionDataInput source, SchemaEvolution.SchemaEvolutionBuilder b) throws IOException {
            b.schema(SchemaInfo.SERIALIZER.deserialize(source))
             .version(VersionInfo.SERIALIZER.deserialize(source))
             .rules(SchemaValidationRules.SERIALIZER.deserialize(source));
        }
    }
}


