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
 * Exception thrown when a serialization format is different from the group property's serialization format. 
 */
public class SerializationFormatMismatchException extends RuntimeException {
    public SerializationFormatMismatchException(String message) {
        super(message);
    }
}
