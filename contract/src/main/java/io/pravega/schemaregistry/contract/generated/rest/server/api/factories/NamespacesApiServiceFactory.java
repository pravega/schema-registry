package io.pravega.schemaregistry.contract.generated.rest.server.api.factories;

import io.pravega.schemaregistry.contract.generated.rest.server.api.NamespacesApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.impl.NamespacesApiServiceImpl;


public class NamespacesApiServiceFactory {
    private final static NamespacesApiService service = new NamespacesApiServiceImpl();

    public static NamespacesApiService getNamespacesApi() {
        return service;
    }
}
