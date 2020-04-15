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
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.VersionInfo;

import java.io.IOException;

public class CompatibilitySerializer extends VersionedSerializer.WithBuilder<Compatibility, Compatibility.CompatibilityBuilder> {
    public static final CompatibilitySerializer SERIALIZER = new CompatibilitySerializer();
    
    @Override
    protected Compatibility.CompatibilityBuilder newBuilder() {
        return Compatibility.builder();
    }

    @Override
    protected byte getWriteVersion() {
        return 0;
    }


    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void write00(Compatibility e, RevisionDataOutput target) throws IOException {
        target.writeCompactInt(e.getCompatibility().ordinal());
        target.writeBoolean(e.getBackwardTill() != null);
        if (e.getBackwardTill() != null) {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getBackwardTill());
        }
        target.writeBoolean(e.getForwardTill() != null);
        if (e.getForwardTill() != null) {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getForwardTill());
        } 
    }
    
    private void read00(RevisionDataInput source, Compatibility.CompatibilityBuilder b) throws IOException {
        int ordinal = source.readCompactInt();
        Compatibility.Type compatibilityType = Compatibility.Type.values()[ordinal];
        b.compatibility(compatibilityType);
        boolean backwardTillSpecified = source.readBoolean();
        if (backwardTillSpecified) {
            VersionInfo backwardTillRecord = VersionInfoSerializer.SERIALIZER.deserialize(source);
            b.backwardTill(backwardTillRecord);
        }
        boolean forwardTillSpecified = source.readBoolean();
        if (forwardTillSpecified) {
            VersionInfo forwardTillRecord = VersionInfoSerializer.SERIALIZER.deserialize(source);
            b.forwardTill(forwardTillRecord);
        }
    }
}
