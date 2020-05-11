/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.contract.data;

import io.pravega.common.ObjectBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Object that captures the version of a schema within a group.
 * {@link VersionInfo#schemaName} object type is same as {@link SchemaInfo#name} which represents the object type 
 * for which the version is computed. 
 * {@link VersionInfo#version} the registry assigned monotonically increasing version number for the schema for specific object type.
 * The version number is per object type, so schema name and version number forms a unique pair. 
 * {@link VersionInfo#ordinal} Absolute ordinal of the schema for all schemas in the group. This uniquely identifies the 
 * schema within a group. 
 */
@Data
@Builder
@AllArgsConstructor
public class VersionInfo {
    private final String schemaName;
    private final int version;
    private final int ordinal;

    public static class VersionInfoBuilder implements ObjectBuilder<VersionInfo> {
    }
}
