/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry;

import io.pravega.schemaregistry.storage.ContinuationToken;
import lombok.Data;

import java.util.Map;

@Data
public class MapWithToken<K, V> {
    private final Map<K, V> map;
    private final ContinuationToken token;
}
