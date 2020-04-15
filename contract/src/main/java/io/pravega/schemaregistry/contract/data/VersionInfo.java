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
 * It contains schema name matching {@link SchemaInfo#name} along with the registry assigned version for the schema in
 * the group. 
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
