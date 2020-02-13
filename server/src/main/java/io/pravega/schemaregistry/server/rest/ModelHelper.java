/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest;

import io.pravega.schemaregistry.contract.SchemaRegistryContract;
import io.pravega.schemaregistry.contract.SchemaValidationRules;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaInfo;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaType;
import io.pravega.schemaregistry.server.rest.generated.model.ValidationRules;

public class ModelHelper {
    public static SchemaRegistryContract.SchemaInfo decode(SchemaInfo schemaInfo) {
        SchemaRegistryContract.SchemaType schemaType = decode(schemaInfo.getSchemaType());
        return new SchemaRegistryContract.SchemaInfo(schemaInfo.getSchemaName(), schemaType, schemaInfo.getSchemaData(), 
                schemaInfo.getProperties());
    }

    private static SchemaRegistryContract.SchemaType decode(SchemaType schemaType) {
        return null;
    }

    public static SchemaValidationRules decode(ValidationRules rules) {
        return null;
    }
}
