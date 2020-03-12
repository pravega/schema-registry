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
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema validation rules that are applied for checking if a schema is valid. 
 * This contains a set of rules. The schema will be compared against one or more existing schemas in the group by applying the rule. 
 */
@Data
@Builder
public class SchemaValidationRules {
    public static final Serializer SERIALIZER = new Serializer();

    private final Map<String, SchemaValidationRule> rules;

    private SchemaValidationRules(Map<String, SchemaValidationRule> rules) {
        this.rules = rules;
    }

    public static SchemaValidationRules of(SchemaValidationRule rule) {
        return new SchemaValidationRules(Collections.singletonMap(rule.getName(), rule));
    }

    public static SchemaValidationRules of(List<SchemaValidationRule> rules) {
        return new SchemaValidationRules(rules.stream().collect(Collectors.toMap(SchemaValidationRule::getName, x -> x)));
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

        @SneakyThrows(IOException.class)
        private void write00(SchemaValidationRules e, RevisionDataOutput target) throws IOException {
            target.writeCompactInt(e.getRules().size());
            for (Map.Entry<String, SchemaValidationRule> rule : e.getRules().entrySet()) {
                target.writeUTF(rule.getKey());
                target.writeArray(SchemaValidationRule.SERIALIZER.toBytes(rule.getValue()));                
            }
        }

        @SneakyThrows(IOException.class)
        private void read00(RevisionDataInput source, SchemaValidationRules.SchemaValidationRulesBuilder b) throws IOException {
            int count = source.readCompactInt();
            Map<String, SchemaValidationRule> rules = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String name = source.readUTF();
                byte[] bytes = source.readArray();
                rules.put(name, SchemaValidationRule.SERIALIZER.fromBytes(bytes));
            }
            b.rules(rules);
        }
    }
}
