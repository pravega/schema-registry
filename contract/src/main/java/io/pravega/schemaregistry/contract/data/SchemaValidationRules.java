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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Schema validation rules that are applied for checking if a schema is valid. 
 * This contains a set of rules and a {@link Compatibility} policy. The schema will be compared against one or more
 * existing schemas in the group by applying the rule. 
 */
@Data
@Builder
@AllArgsConstructor
public class SchemaValidationRules {
    public static final Serializer SERIALIZER = new Serializer();

    private final ImmutableList<SchemaValidationRule> rules;
    private final Compatibility compatibility;

    public static SchemaValidationRules of() {
        return new SchemaValidationRules(null, null);    
    }
    
    private static class SchemaValidationRulesBuilder implements ObjectBuilder<SchemaValidationRules> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<SchemaValidationRules, SchemaValidationRules.SchemaValidationRulesBuilder> {
        @Override
        protected SchemaValidationRules.SchemaValidationRulesBuilder newBuilder() {
            return SchemaValidationRules.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SchemaValidationRules e, RevisionDataOutput target) throws IOException {
            Compatibility.SERIALIZER.serialize(target, e.compatibility);
        }

        private void read00(RevisionDataInput source, SchemaValidationRules.SchemaValidationRulesBuilder b) throws IOException {
            b.rules(ImmutableList.of())
             .compatibility(Compatibility.SERIALIZER.deserialize(source));
        }
    }
}
