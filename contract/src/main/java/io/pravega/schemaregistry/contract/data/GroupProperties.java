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

import lombok.Data;

@Data
public class GroupProperties {
    private final SchemaType schemaType;
    private final SchemaValidationRules schemaValidationRules;
    private final boolean subgroupBySchemaName;
    private final boolean enableEncoding;
}
