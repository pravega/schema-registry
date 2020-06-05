/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.exceptions;

/**
 * Exception thrown when a codec type is not registered for the group. 
 */
public class CodecTypeNotRegisteredException extends RuntimeException {
    public CodecTypeNotRegisteredException(String message) {
        super(message);
    }
}
