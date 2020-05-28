package io.pravega.schemaregistry.contract.generated.rest.server.api.factories;

import io.pravega.schemaregistry.contract.generated.rest.server.api.SchemasApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.impl.SchemasApiServiceImpl;


public class SchemasApiServiceFactory {
    private final static SchemasApiService service = new SchemasApiServiceImpl();

    public static SchemasApiService getSchemasApi() {
        return service;
    }
}
