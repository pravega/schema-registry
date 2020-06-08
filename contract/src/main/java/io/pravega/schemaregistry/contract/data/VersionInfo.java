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
 * Version information object that encapsulates properties that uniquely identify a specific version of a schema within a group.
 * 
 * {@link VersionInfo#type} is same as {@link SchemaInfo#type} which represents the object type for which the version is computed. 
 * {@link VersionInfo#version} the registry assigned monotonically increasing version number for the schema for specific object type.
 * Since the version number is per object type, so type and version number forms a unique pair. 
 * {@link VersionInfo#ordinal} Absolute ordinal of the schema for all schemas in the group. This uniquely identifies the 
 * version within a group. 
 */
@Data
@Builder
@AllArgsConstructor
public class VersionInfo {
    /**
     * Object type which is declared in the corresponding {@link SchemaInfo#type} for the schemainfo that is identified 
     * by this version info. 
     */
    private final String type;
    /**
     * A version number that identifies the position of schema among other schemas in the group that share the same 'type'.
     */
    private final int version;
    /**
     * A position identifier that uniquely identifies the schema within a group and represents the order in which this
     * schema was included in the group. 
     */
    private final int ordinal;

    public static class VersionInfoBuilder implements ObjectBuilder<VersionInfo> {
    }
}
