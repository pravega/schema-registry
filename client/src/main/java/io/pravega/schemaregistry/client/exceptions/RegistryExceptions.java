/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.client.exceptions;

import com.google.common.base.Preconditions;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class RegistryExceptions extends RuntimeException {
    /**
     * Enum to describe the type of exception.
     */
    public enum Type {
        UNAUTHORIZED,
        BAD_ARGUMENT,
        PRECONDITION_FAILED,
        CODEC_NOT_FOUND,
        MALFORMED_SCHEMA,
        INCOMPATIBLE_SCHEMA,
        RESOURCE_NOT_FOUND,
        SERIALIZATION_FORMAT_MISMATCH,
        CONNECTION_ERROR,
        INTERNAL_SERVER_ERROR
    }

    /**
     * Trait to identify whether an exception is retryable or not. 
     */
    public interface RetryableException {
    }

    /**
     * Construct a StoreException.
     *
     * @param errorMessage The detailed error message.
     */
    public RegistryExceptions(final String errorMessage) {
        super(errorMessage);
    }
    
    /**
     * Factory method to construct Store exceptions.
     *
     * @param type         Type of Exception.
     * @param errorMessage The detailed error message.
     * @return Instance of type of StoreException.
     */
    public static RegistryExceptions create(final Type type, final String errorMessage) {
        Preconditions.checkArgument(errorMessage != null && !errorMessage.isEmpty(),
                "Either cause or errorMessage should be non-empty");
        RegistryExceptions exception;
        switch (type) {
            case UNAUTHORIZED:
                exception = new UnauthorizedException(errorMessage);
                break;
            case BAD_ARGUMENT:
                exception = new BadArgumentException(errorMessage);
                break;
            case PRECONDITION_FAILED:
                exception = new PreconditionFailedException(errorMessage);
                break;
            case CODEC_NOT_FOUND:
                exception = new CodecTypeNotRegisteredException(errorMessage);
                break;
            case INCOMPATIBLE_SCHEMA:
                exception = new SchemaValidationFailedException(errorMessage);
                break;
            case RESOURCE_NOT_FOUND:
                exception = new ResourceNotFoundException(errorMessage);
                break;
            case SERIALIZATION_FORMAT_MISMATCH:
                exception = new SerializationMismatchException(errorMessage);
                break;
            case CONNECTION_ERROR:
                exception = new ConnectionException(errorMessage);
                break;
            case INTERNAL_SERVER_ERROR:
                exception = new InternalServerError(errorMessage);
                break;
            default:
                throw new IllegalArgumentException("Invalid exception type");
        }
        return exception;
    }

    /**
     * User is unauthorized to perform requested action. 
     */
    public static class UnauthorizedException extends RegistryExceptions {
        public UnauthorizedException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * Service rejected the supplied arguments with bad argument exception. 
     */
    public static class BadArgumentException extends RegistryExceptions {
        public BadArgumentException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * Service rejected the request because the expected precondition for the requested action was not satisfied.
     */
    public static class PreconditionFailedException extends RegistryExceptions {
        public PreconditionFailedException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * The requested codecType is not added to the group.
     */
    public static class CodecTypeNotRegisteredException extends RegistryExceptions {
        public CodecTypeNotRegisteredException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * Schema is malformed. Verify the schema data and type. 
     */
    public static class MalformedSchemaException extends RegistryExceptions {
        public MalformedSchemaException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * The schema validation failed as it was validated against the ValidationRules set for the group.
     */
    public static class SchemaValidationFailedException extends RegistryExceptions {
        public SchemaValidationFailedException(String errorMessage) {
            super(errorMessage);
        }
    }
    
    /**
     * Requested resource not found.
     */
    public static class ResourceNotFoundException extends RegistryExceptions {
        public ResourceNotFoundException(String errorMessage) {
            super(errorMessage);
        }
    }
    
    /**
     * Serialization format is not allowed for the group. Check {@link SchemaInfo#serializationFormat} with 
     * {@link GroupProperties#serializationFormat}. 
     */
    public static class SerializationMismatchException extends RegistryExceptions {
        public SerializationMismatchException(String errorMessage) {
            super(errorMessage);
        }
    }
    
    /**
     * Exception type due to failure in connecting to the service.
     */
    public static class ConnectionException extends RegistryExceptions implements RetryableException {
        public ConnectionException(String errorMessage) {
            super(errorMessage);
        }
    }

    /**
     * The request processing failed on the service.
     */
    public static class InternalServerError extends RegistryExceptions implements RetryableException {
        public InternalServerError(String errorMessage) {
            super(errorMessage);
        }
    }
}
