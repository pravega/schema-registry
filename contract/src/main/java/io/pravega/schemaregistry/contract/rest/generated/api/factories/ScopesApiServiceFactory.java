package io.pravega.schemaregistry.contract.rest.generated.api.factories;

import io.pravega.schemaregistry.contract.rest.generated.api.ScopesApiService;
import io.pravega.schemaregistry.contract.rest.generated.api.impl.ScopesApiServiceImpl;


public class ScopesApiServiceFactory {
    private final static ScopesApiService service = new ScopesApiServiceImpl();

    public static ScopesApiService getScopesApi() {
        return service;
    }
}
