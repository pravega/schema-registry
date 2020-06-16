/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.resources;

import io.pravega.common.Exceptions;
import io.pravega.schemaregistry.contract.generated.rest.model.AddedTo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.transform.ModelHelper;
import io.pravega.schemaregistry.contract.v1.ApiV1;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.StoreExceptions;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
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

    public SchemaResourceImpl(SchemaRegistryService registryService, ServiceConfig config, Executor executor) {
        super(registryService, config, executor);
    }

    @Override
    public void getSchemaReferences(SchemaInfo schemaInfo, String namespace, AsyncResponse asyncResponse) {
        withCompletion("getSchemaReferences", READ, String.format(AuthResources.NAMESPACE_FORMAT, namespace), asyncResponse,
                () -> getRegistryService().getSchemaReferences(namespace, ModelHelper.decode(schemaInfo))
                                          .thenApply(map -> {
                                              AddedTo addedTo = new AddedTo()
                                                      .groups(map.entrySet().stream().collect(
                                                              Collectors.toMap(Map.Entry::getKey,
                                                                      x -> ModelHelper.encode(x.getValue()))));
                                              log.info("getSchemaReferences {} ", map.keySet());
                                              return Response.status(Status.OK).entity(addedTo).build();
                                          })
                                          .exceptionally(exception -> {
                                              if (Exceptions.unwrap(exception) instanceof StoreExceptions.DataNotFoundException) {
                                                  log.warn("Schema {} not found", schemaInfo.getType());
                                                  return Response.status(Status.NOT_FOUND).build();
                                              }
                                              log.warn("getCodecTypesList failed with exception: ", exception);
                                              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
                                          }))
                .thenApply(response -> {
                    asyncResponse.resume(response);
                    return response;
                });
    }
}
