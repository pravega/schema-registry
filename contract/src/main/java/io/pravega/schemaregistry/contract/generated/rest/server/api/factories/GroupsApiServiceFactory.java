package io.pravega.schemaregistry.contract.generated.rest.server.api.factories;

import io.pravega.schemaregistry.contract.generated.rest.server.api.GroupsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.impl.GroupsApiServiceImpl;


public class GroupsApiServiceFactory {
    private final static GroupsApiService service = new GroupsApiServiceImpl();

    public static GroupsApiService getGroupsApi() {
        return service;
    }
}
