/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage;

/**
 * Pointer to a position/offset in the {@link io.pravega.schemaregistry.storage.impl.group.Log} where any record has been written. 
 * @param <T> Type of position. 
 */
public interface Position<T extends Comparable<T>> {
    T getPosition();
}
