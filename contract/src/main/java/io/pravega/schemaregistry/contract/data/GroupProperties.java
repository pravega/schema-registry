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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

/**
 * Different configuration choices for a group. 
 * 
 * {@link GroupProperties#serializationFormat} identifies the serialization format and schema type used to describe the schema.
 * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
 * {@link GroupProperties#allowMultipleTypes} that specifies if multiple schemas with distinct {@link SchemaInfo#type} 
 * are allowed to coexist within the group. A schema describes an object and each object type is distinctly identified by
 * {@link SchemaInfo#type}. Registry service validates new schema with existing schema versions of the same name and versions
 * it accordingly. Allowing multiple schemas, each versioned independently, allows applications to use schema registry groups
 * for streaming scenarios like event sourcing, or message bus where different types of events could be written to the same
 * stream. Similarly, a group with multiple schemas can be used to describe a database catalog with each schema representing 
 * a different table. 
 * The users can register new versions of each distinct type of schema, and the registry will check for compatibility 
 * for each type independently.
 * {@link GroupProperties#properties} This is general purpose key value string to include any additional user defined information for the group. 
 */
@Data
@Builder
@AllArgsConstructor
public class GroupProperties {
    private final SerializationFormat serializationFormat;
    private final SchemaValidationRules schemaValidationRules;
    private final boolean allowMultipleTypes;
    private final Map<String, String> properties;

    public static final class GroupPropertiesBuilder {
        private SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.fullTransitive());
        private boolean allowMultipleTypes = false;
        private Map<String, String> properties = Collections.emptyMap();

        public GroupPropertiesBuilder compatibility(Compatibility compatibility) {
            this.schemaValidationRules = SchemaValidationRules.of(compatibility);
            return this;
        }
    }
}