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
