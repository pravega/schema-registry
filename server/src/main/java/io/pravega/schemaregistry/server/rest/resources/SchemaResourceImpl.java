/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.schemaregistry.contract.generated.rest.model.AddedTo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.server.rest.auth.AuthHandlerManager;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static javax.ws.rs.core.Response.Status;

/**
 * Schema Registry Resource implementation.
 */
@Slf4j
public class SchemaResourceImpl extends AbstractResource implements ApiV1.SchemasApiAsync {
    private SchemaRegistryService registryService;

    public SchemaResourceImpl(SchemaRegistryService registryService, ServiceConfig config, AuthHandlerManager authManager, Executor executor) {
        super(registryService, config, authManager, executor);
    }

    @Override
    public void getSchemaReferences(SchemaInfo schemaInfo, String namespace, SecurityContext securityContext, AsyncResponse asyncResponse) {
        withAuthorization(READ, getNamespaceResource(namespace), asyncResponse,
                () -> getRegistryService().getSchemaReferences(namespace, ModelHelper.decode(schemaInfo))
                                          .thenApply(map -> {
                                              AddedTo addedTo = new AddedTo()
                                                      .groups(map.entrySet().stream().collect(
                                                              Collectors.toMap(Map.Entry::getKey,
                                                                      x -> ModelHelper.encode(x.getValue()))));
                                              log.info("getSchemaReferences {} ", map.keySet());
                                              return Response.status(Status.OK).entity(addedTo).build();
                                          }), 
                securityContext, () -> String.format("getSchemaReferences for namespace %s failed with exception:", namespace))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }
}
