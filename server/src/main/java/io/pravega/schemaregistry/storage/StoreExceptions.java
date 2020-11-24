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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class StoreExceptions extends RuntimeException {

    /**
     * Enum to describe the type of exception.
     */
    public enum Type {
        DATA_EXISTS,
        DATA_NOT_FOUND,
        DATA_CONTAINER_NOT_FOUND,
        DATA_NOT_EMPTY,
        WRITE_CONFLICT,
        CONNECTION_ERROR,
        AUTH_ERROR,
        UNKNOWN
    }

    /**
     * Construct a StoreException.
     *
     * @param errorMessage The detailed error message.
     * @param cause        Exception cause.
     */
    private StoreExceptions(final String errorMessage, final Throwable cause) {
        super(errorMessage, cause);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type  Type of Exception.
     * @param cause Exception cause.
     * @return Instance of StoreException.
     */
    public static StoreExceptions create(final Type type, final Throwable cause) {
        Preconditions.checkNotNull(cause, "cause");
        return create(type, cause, null);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type         Type of Exception.
     * @param errorMessage The detailed error message.
     * @return Instance of StoreException.
     */
    public static StoreExceptions create(final Type type, final String errorMessage) {
        Exceptions.checkNotNullOrEmpty(errorMessage, "errorMessage");
        return create(type, null, errorMessage);
    }

    /**
     * Factory method to construct Store exceptions.
     *
     * @param type         Type of Exception.
     * @param cause        Exception cause.
     * @param errorMessage The detailed error message.
     * @return Instance of type of StoreException.
     */
    public static StoreExceptions create(final Type type, final Throwable cause, final String errorMessage) {
        Preconditions.checkArgument(cause != null || (errorMessage != null && !errorMessage.isEmpty()),
                "Either cause or errorMessage should be non-empty");
        StoreExceptions exception;
        switch (type) {
            case DATA_EXISTS:
                exception = new DataExistsException(errorMessage, cause);
                break;
            case DATA_NOT_FOUND:
                exception = new DataNotFoundException(errorMessage, cause);
                break;
            case DATA_CONTAINER_NOT_FOUND:
                exception = new DataContainerNotFoundException(errorMessage, cause);
                break;
            case DATA_NOT_EMPTY:
                exception = new DataNotEmptyException(errorMessage, cause);
                break;
            case WRITE_CONFLICT:
                exception = new WriteConflictException(errorMessage, cause);
                break;
            case CONNECTION_ERROR:
                exception = new StoreConnectionException(errorMessage, cause);
                break;
            case AUTH_ERROR:
                exception = new TokenException(errorMessage, cause);
                break;
            case UNKNOWN:
                exception = new UnknownException(errorMessage, cause);
                break;
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
        return exception;
    }

    /**
     * Exception type when node exists, and duplicate node is created.
     */
    public static class DataExistsException extends StoreExceptions {
        private DataExistsException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when node does not exist and is operated on.
     */
    public static class DataNotFoundException extends StoreExceptions {
        private DataNotFoundException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when node does not exist and is operated on.
     */
    public static class DataContainerNotFoundException extends StoreExceptions {
        private DataContainerNotFoundException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when deleting a non empty node.
     */
    public static class DataNotEmptyException extends StoreExceptions {
        private DataNotEmptyException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when you are attempting to update a stale value.
     */
    public static class WriteConflictException extends StoreExceptions implements RetryableException {
        private WriteConflictException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }
    
    /**
     * Exception type due to failure in connecting to the store.
     */
    public static class StoreConnectionException extends StoreExceptions implements RetryableException {
        private StoreConnectionException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type due to failure in authenticating with store.
     */
    public static class TokenException extends StoreExceptions implements RetryableException {
        private TokenException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }

    /**
     * Exception type when the cause is not known.
     */
    public static class UnknownException extends StoreExceptions {
        private UnknownException(String errorMessage, Throwable cause) {
            super(errorMessage, cause);
        }
    }
}
