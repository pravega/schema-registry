/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import io.pravega.schemaregistry.contract.data.SchemaValidationRules;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema validation rules that are applied for checking if a schema is valid. 
 * This contains a set of rules. The schema will be compared against one or more existing schemas in the group by applying the rule. 
 */
public class SchemaValidationRulesSerializer extends VersionedSerializer.WithBuilder<SchemaValidationRules, SchemaValidationRules.SchemaValidationRulesBuilder> {
    public static final SchemaValidationRulesSerializer SERIALIZER = new SchemaValidationRulesSerializer();
    
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
            ByteBuffer buffer = SchemaValidationRuleRecord.SERIALIZER.toBytes(rule.getValue());
            byte[] array = new byte[buffer.remaining()];
            buffer.get(array);

            target.writeArray(array);
        }
    }

    @SneakyThrows(IOException.class)
    private void read00(RevisionDataInput source, SchemaValidationRules.SchemaValidationRulesBuilder b) throws IOException {
        int count = source.readCompactInt();
        Map<String, SchemaValidationRule> rules = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String name = source.readUTF();
            byte[] bytes = source.readArray();
            SchemaValidationRule schemaValidationRule = SchemaValidationRuleRecord.SERIALIZER.fromBytes(ByteBuffer.wrap(bytes));
            rules.put(name, schemaValidationRule);
        }
        b.rules(rules);
    }
}
