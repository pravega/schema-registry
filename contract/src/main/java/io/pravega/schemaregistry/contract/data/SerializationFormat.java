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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Different types of serialization formats used for serializing data. 
 * Registry supports Avro, Protobuf and Json serialization formats but any custom type could be used with the registry using custom type. 
 *
 * If a serialization format is not present in the enum it can be specified using {@link SerializationFormat#custom} with 
 * {@link SerializationFormat#fullTypeName}. 
 * Allowed values of {@link Compatibility} with {@link SerializationFormat#custom} are {@link Compatibility#allowAny}  
 * or {@link Compatibility#denyAll}.
 */

public enum SerializationFormat {
    Avro,
    Protobuf,
    Json,
    Any,
    Custom;

    @Getter
    @Setter(AccessLevel.PRIVATE)
    private String fullTypeName;

    /**
     * Method to define a custom serialization format with a custom name. 
     * @param fullTypeName Custom type name. 
     * @return {@link SerializationFormat#Custom} with supplied custom type name. 
     */
    public static SerializationFormat custom(String fullTypeName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fullTypeName));
        SerializationFormat type = SerializationFormat.Custom;
        type.setFullTypeName(fullTypeName);
        return type;
    }

    /**
     * Method to create a serialization format with a full name.
     * 
     * @param fullTypeName Custom type name. 
     * @param format Serialization format.
     * @return {@link SerializationFormat#Custom} with supplied custom type name. 
     */
    public static SerializationFormat withName(SerializationFormat format, String fullTypeName) {
        Preconditions.checkArgument(format != null);
        SerializationFormat type = SerializationFormat.valueOf(format.name());
        type.setFullTypeName(fullTypeName);
        return type;
    }
}
