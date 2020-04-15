/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.schemaregistry.storage.impl.group.records;

import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.contract.data.SchemaInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SchemaInfoSerializer extends VersionedSerializer.WithBuilder<SchemaInfo, SchemaInfo.SchemaInfoBuilder> {
    public static final SchemaInfoSerializer SERIALIZER = new SchemaInfoSerializer();

    @Override
    protected SchemaInfo.SchemaInfoBuilder newBuilder() {
        return SchemaInfo.builder();
    }

    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void write00(SchemaInfo e, RevisionDataOutput target) throws IOException {
        target.writeUTF(e.getName());
        SchemaTypeRecord.SERIALIZER.serialize(target, new SchemaTypeRecord(e.getSchemaType()));
        target.writeArray(e.getSchemaData());
        target.writeMap(e.getProperties(), DataOutput::writeUTF, DataOutput::writeUTF);
    }

    private void read00(RevisionDataInput source, SchemaInfo.SchemaInfoBuilder b) throws IOException {
        b.name(source.readUTF())
         .schemaType(SchemaTypeRecord.SERIALIZER.deserialize(source).getSchemaType())
         .schemaData(source.readArray())
         .properties(source.readMap(DataInput::readUTF, DataInput::readUTF));
    }
}
