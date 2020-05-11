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
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.io.IOException;

/**
 * Object that captures the version of a schema within a group.
 * It contains schema name matching {@link SchemaInfo#name} along with the registry assigned version for the schema in
 * the group. 
 */
public class VersionInfoSerializer extends VersionedSerializer.WithBuilder<VersionInfo, VersionInfo.VersionInfoBuilder> {
    public static final VersionInfoSerializer SERIALIZER = new VersionInfoSerializer();
    
    @Override
    protected VersionInfo.VersionInfoBuilder newBuilder() {
        return VersionInfo.builder();
    }

    @Override
    protected byte getWriteVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void write00(VersionInfo e, RevisionDataOutput target) throws IOException {
        target.writeUTF(e.getSchemaName());
        target.writeInt(e.getVersion());
        target.writeInt(e.getOrdinal());
    }

    private void read00(RevisionDataInput source, VersionInfo.VersionInfoBuilder b) throws IOException {
        b.schemaName(source.readUTF())
         .version(source.readInt())
         .ordinal(source.readInt());
    }
}
