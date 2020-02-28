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

/**
 * Base interface to define all schema validation rules. Schema validation rules are applied whenever new schemas are registered
 * and only schemas that satisfy validation rules are accepted by the registry into the group.  
 */
public interface SchemaValidationRule {
    /**
     * Name of the rule to identify it with. 
     * 
     * @return name of the rule. 
     */
    String getName();
}
