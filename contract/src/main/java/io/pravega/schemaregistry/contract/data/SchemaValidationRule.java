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

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base interface to define all schema validation rules. Schema validation rules are applied whenever new schemas are registered
 * and only schemas that satisfy validation rules are accepted by the registry into the group.  
 */
public interface SchemaValidationRule {
    Serializer SERIALIZER = new Serializer();

    /**
     * Name of the rule to identify it with. 
     * 
     * @return name of the rule. 
     */
    String getName();

    class Serializer extends VersionedSerializer.MultiType<SchemaValidationRule> {

        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(Compatibility.class, 1, new Compatibility.Serializer());
        }

        /**
         * Serializes the given {@link SchemaValidationRule} to a {@link ByteBuffer}.
         *
         * @param value The {@link SchemaValidationRule} to serialize.
         * @return A new {@link ByteBuffer} wrapping an array that contains the serialization.
         */
        @SneakyThrows(IOException.class)
        public byte[] toBytes(SchemaValidationRule value) {
            ByteArraySegment s = serialize(value);
            return s.getCopy();
        }

        /**
         * Deserializes the given {@link ByteBuffer} into a {@link SchemaValidationRule} instance.
         *
         * @param buffer {@link ByteBuffer} to deserialize.
         * @return A new {@link SchemaValidationRule} instance from the given serialization.
         */
        @SneakyThrows(IOException.class)
        public SchemaValidationRule fromBytes(byte[] buffer) {
            return deserialize(new ByteArraySegment(buffer));
        }
    }

}
