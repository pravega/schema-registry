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
 * {@link GroupProperties#schemaType} identifies the serialization format and schema type used to describe the schema.
 * {@link GroupProperties#schemaValidationRules} sets the schema validation policy that needs to be enforced for evolving schemas.
 * {@link GroupProperties#validateByObjectType} specifies that if schemas have validation rules applied for schemas that share the 
 * same {@link SchemaInfo#name} which represents the object type. This ensures that the registry can support scenarios like 
 * event sourcing, or message bus where different types of events could be written to the same
 * stream. The users can register new versions of each distinct type of schema, and the registry will check for compatibility 
 * for each type independently.
 * If validateByObjectType is set to true, then schemas are validate against other schemas in the group that share the same 
 * {@link SchemaInfo#name}.  
 * {@link GroupProperties#properties} This is general purpose properties bag to include any additional metadata for the group. 
 * For example, a group properties can include information about whether encodingIds are written as header along with data payload.  
 * Its usage in pravega clients is to indicate the same. The writer and reader clients may look for specific properties to determine 
 * how to decode the payload in the stream. 
 * This allows schema registry to be used with pravega streams with applications optionally choosing to include encoding information 
 * with the payload. This is useful for serialization formats where it is not mandatory to share writer schema at the read time 
 * while still accruing other benefits of registry service such as declaration of structure of data in the stream and evolution
 * of schemas in conformance with schema validation rules. 
 */
@Data
@Builder
@AllArgsConstructor
public class GroupProperties {
    private final SchemaType schemaType;
    private final SchemaValidationRules schemaValidationRules;
    private final boolean validateByObjectType;
    private final Map<String, String> properties;

    public static final class GroupPropertiesBuilder {
        private SchemaValidationRules schemaValidationRules = SchemaValidationRules.of(Compatibility.fullTransitive());
        private boolean validateByObjectType = false;
        private Map<String, String> properties = Collections.emptyMap();

        public GroupPropertiesBuilder compatibility(Compatibility compatibility) {
            this.schemaValidationRules = SchemaValidationRules.of(compatibility);
            return this;
        }
    }
}