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
    /**
     * Map of schema validation rule name to corresponding schema validation rule.  
     */
    private final Map<String, SchemaValidationRule> rules;

    private SchemaValidationRules(Map<String, SchemaValidationRule> rules) {
        this.rules = rules;
    }

    /**
     * Method to create a rule for compatibility.
     * 
     * @param compatibility compatibility policy to be used. 
     * @return A singleton rules map containing the compatibility rule. 
     */
    public static SchemaValidationRules of(Compatibility compatibility) {
        return new SchemaValidationRules(Collections.singletonMap(compatibility.getName(), compatibility));
    }

    /**
     * Method to create SchemaValidationRules from the list of supplied rules. If multiple same rule are present 
     * in the list then only the latest rule of each type is added to the Rules map. 
     * Currently the only rule supported is {@link Compatibility}. 
     * 
     * @param rules List of rules. 
     * @return SchemaValidationRules object. 
     */
    public static SchemaValidationRules of(List<SchemaValidationRule> rules) {
        Preconditions.checkNotNull(rules);
        Preconditions.checkArgument(rules.stream().allMatch(x -> x instanceof Compatibility), "Only compatibility rule is supported.");
        return new SchemaValidationRules(rules.stream().collect(Collectors.toMap(SchemaValidationRule::getName, x -> x)));
    }

    public static class SchemaValidationRulesBuilder implements ObjectBuilder<SchemaValidationRules> {
    }

}
