package io.pravega.schemaregistry.contract.generated.rest.server.api.factories;

import io.pravega.schemaregistry.contract.generated.rest.server.api.ScopesApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.impl.ScopesApiServiceImpl;


public class ScopesApiServiceFactory {
    private final static ScopesApiService service = new ScopesApiServiceImpl();

    public static ScopesApiService getScopesApi() {
        return service;
    }
}
