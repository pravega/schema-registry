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
        if (e.getBackwardTill() != null) {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getBackwardTill());
        } else {
            VersionInfoSerializer.SERIALIZER.serialize(target, VersionInfoSerializer.NON_EXISTENT);
        }
        if (e.getForwardTill() != null) {
            VersionInfoSerializer.SERIALIZER.serialize(target, e.getForwardTill());
        } else {
            VersionInfoSerializer.SERIALIZER.serialize(target, VersionInfoSerializer.NON_EXISTENT);
        }
    }
    
    private void read00(RevisionDataInput source, Compatibility.CompatibilityBuilder b) throws IOException {
        int ordinal = source.readCompactInt();
        Compatibility.Type compatibilityType = Compatibility.Type.values()[ordinal];
        b.compatibility(compatibilityType);
        VersionInfo backwardTillRecord = VersionInfoSerializer.SERIALIZER.deserialize(source);
        VersionInfo backwardTill = backwardTillRecord.equals(VersionInfoSerializer.NON_EXISTENT) ? null : backwardTillRecord;
        b.backwardTill(backwardTill);
        VersionInfo forwardTillRecord = VersionInfoSerializer.SERIALIZER.deserialize(source);
        VersionInfo forwardTill = forwardTillRecord.equals(VersionInfoSerializer.NON_EXISTENT) ? null : forwardTillRecord;
        b.forwardTill(forwardTill);
    }
}
