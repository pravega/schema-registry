/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableKeySerializer extends VersionedSerializer.MultiType<TableRecords.TableKey> {

    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(TableRecords.VersionKey.class, 1, new TableRecords.VersionKey.Serializer())
               .serializer(TableRecords.ValidationPolicyKey.class, 2, new TableRecords.ValidationPolicyKey.Serializer())
               .serializer(TableRecords.Etag.class, 3, new TableRecords.Etag.Serializer())
               .serializer(TableRecords.GroupPropertyKey.class, 4, new TableRecords.GroupPropertyKey.Serializer())
               .serializer(TableRecords.SchemaFingerprintKey.class, 5, new TableRecords.SchemaFingerprintKey.Serializer())
               .serializer(TableRecords.EncodingInfoRecord.class, 6, new TableRecords.EncodingInfoRecord.Serializer())
               .serializer(TableRecords.EncodingIdRecord.class, 7, new TableRecords.EncodingIdRecord.Serializer())
               .serializer(TableRecords.LatestEncodingIdKey.class, 8, new TableRecords.LatestEncodingIdKey.Serializer())
               .serializer(TableRecords.LatestSchemaVersionKey.class, 9, new TableRecords.LatestSchemaVersionKey.Serializer())
               .serializer(TableRecords.LatestSchemaVersionForTypeKey.class, 10, new TableRecords.LatestSchemaVersionForTypeKey.Serializer())
               .serializer(TableRecords.CodecTypesKey.class, 11, new TableRecords.CodecTypesKey.Serializer())
               .serializer(TableRecords.SchemaTypesKey.class, 12, new TableRecords.SchemaTypesKey.Serializer())
               .serializer(TableRecords.SchemaTypeVersionKey.class, 13, new TableRecords.SchemaTypeVersionKey.Serializer())
               .serializer(TableRecords.VersionDeletedRecord.class, 14, new TableRecords.VersionDeletedRecord.Serializer());
    }

    /**
     * Serializes the given {@link TableRecords.TableKey} to a {@link ByteBuffer}.
     *
     * @param value The {@link TableRecords.TableKey} to serialize.
     * @return An array that contains the serialized key.
     */
    @SneakyThrows(IOException.class)
    public byte[] toBytes(TableRecords.TableKey value) {
        ByteArraySegment s = serialize(value);
        return s.getCopy();
    }
    
    /**
     * Deserializes the given buffer into a {@link TableRecords.TableKey} instance.
     *
     * @param buffer buffer to deserialize into key.
     * @return A new {@link TableRecords.TableKey} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public TableRecords.TableKey fromBytes(byte[] buffer) {
        return deserialize(new ByteArraySegment(buffer));
    }
}
