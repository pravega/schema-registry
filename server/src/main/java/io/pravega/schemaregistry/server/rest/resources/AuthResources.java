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

import io.pravega.schemaregistry.service.Config;

class AuthResources {
    static final String DOMAIN = Config.AUTH_RESOURCE_QUALIFIER;
    static final String DEFAULT_NAMESPACE = "";
    static final String NAMESPACE_FORMAT = DOMAIN + "/%s";
    static final String NAMESPACE_GROUP_FORMAT = NAMESPACE_FORMAT + "/%s";
    static final String NAMESPACE_GROUP_SCHEMA_FORMAT = NAMESPACE_GROUP_FORMAT + "/schemas";
    static final String NAMESPACE_GROUP_CODEC_FORMAT = NAMESPACE_GROUP_FORMAT + "/codecs";
}
