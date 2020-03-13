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

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.SchemaValidationRule;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SchemaValidationRuleRecord {
    Serializer SERIALIZER = new Serializer();
    
    class Serializer extends VersionedSerializer.MultiType<SchemaValidationRule> {

        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(Compatibility.class, 1, new CompatibilitySerializer());
        }

        /**
         * Serializes the given {@link SchemaValidationRuleRecord} to a {@link ByteBuffer}.
         *
         * @param value The {@link SchemaValidationRuleRecord} to serialize.
         * @return A new {@link ByteBuffer} wrapping an array that contains the serialization.
         */
        @SneakyThrows(IOException.class)
        public byte[] toBytes(SchemaValidationRule value) {
            ByteArraySegment s = serialize(value);
            return s.getCopy();
        }

        /**
         * Deserializes the given {@link ByteBuffer} into a {@link SchemaValidationRuleRecord} instance.
         *
         * @param buffer {@link ByteBuffer} to deserialize.
         * @return A new {@link SchemaValidationRuleRecord} instance from the given serialization.
         */
        @SneakyThrows(IOException.class)
        public SchemaValidationRule fromBytes(byte[] buffer) {
            return deserialize(new ByteArraySegment(buffer));
        }
    }

}
