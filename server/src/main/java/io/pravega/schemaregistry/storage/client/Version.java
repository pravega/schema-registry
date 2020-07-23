/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.client;

import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@EqualsAndHashCode
public class Version {
    public static final Version NON_EXISTENT = new Version(TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion());
    public static final Version NO_VERSION = new Version(TableSegmentKeyVersion.NO_VERSION.getSegmentVersion());
    
    private final long version;
    
    public long toLong() {
        return version;
    }
}
