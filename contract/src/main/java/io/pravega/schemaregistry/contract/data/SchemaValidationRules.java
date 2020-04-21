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
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema validation rules that are applied for checking if a schema is valid. 
 * This contains a set of rules {@link SchemaValidationRule}. Currently the only rule that is supported is {@link Compatibility}.
 * The schema will be compared against one or more existing schemas in the group by checking it for satisfying each of the 
 * rules. 
 */
@Data
@Builder
public class SchemaValidationRules {
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

    public static class SchemaValidationRulesBuilder implements ObjectBuilder<SchemaValidationRules> {
    }

}
