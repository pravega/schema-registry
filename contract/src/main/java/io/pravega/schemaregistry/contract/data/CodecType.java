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
import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import lombok.Builder;
import lombok.Data;

/**
 * Encapsulates properties of a codecType.  
 */
@Data
@Builder
public class CodecType {
    public static final CodecType NONE = new CodecType("");

    /**
     * Name that identifies the codec type. Users could typically use the mime type name for the encoding.   
     */
    private final String name;
    /**
     * User defined key value strings that users can use to add any additional metadata to the codecType. 
     * This can be used to share additional information with the decoder about how to decode, for example, if codecType was
     * for encryption, the additional information could include algorithm and other params required for decryption.
     * This is opaque to the service and stored with the codecType when the codec is registered for a group and delivered
     * with encoding information. 
     */
    private final ImmutableMap<String, String> properties;

    public CodecType(String name) {
        this(name, ImmutableMap.of());
    }

    public CodecType(String name, ImmutableMap<String, String> properties) {
        Preconditions.checkArgument(name != null);
        this.name = name;
        this.properties = properties;
    }
    
    public static class CodecTypeBuilder implements ObjectBuilder<CodecType> {
    }
}
