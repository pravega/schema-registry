/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.records;

import io.pravega.client.state.Revision;
import io.pravega.schemaregistry.storage.Position;
import lombok.Data;

@Data
public class PravegaPosition implements Position<Revision> {
    private final Revision revision;
    
    @Override
    public Revision getPosition() {
        return revision;
    }
}
