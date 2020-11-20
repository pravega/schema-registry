/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.server.rest.auth;

import io.pravega.auth.AuthHandler;
import io.pravega.auth.ServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.server.security.auth.handler.impl.PasswordAuthHandler;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.Config;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicAuthHandler extends PasswordAuthHandler implements AuthHandler {
    @Override
    public void initialize(ServerConfig serverConfig) {
        super.initialize(GRPCServerConfigImpl
                .builder().port(Config.SERVICE_PORT)
                .userPasswordFile(((ServiceConfig) serverConfig).getUserPasswordFilePath()).build());
    }
}



