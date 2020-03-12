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

import io.pravega.client.state.Revision;
import io.pravega.client.state.impl.RevisionImpl;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.schemaregistry.storage.Position;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
public class PravegaPosition implements Position<Revision> {
    public static final Serializer SERIALIZER = new Serializer();

    private final Revision revision;
    
    @Override
    public Revision getPosition() {
        return revision;
    }
    
    private static class PravegaPositionBuilder implements ObjectBuilder<PravegaPosition> {
    }

    static class Serializer extends VersionedSerializer.WithBuilder<PravegaPosition, PravegaPosition.PravegaPositionBuilder> {
        @Override
        protected PravegaPosition.PravegaPositionBuilder newBuilder() {
            return PravegaPosition.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(PravegaPosition e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.revision.asImpl().toString());   
        }

        private void read00(RevisionDataInput source, PravegaPosition.PravegaPositionBuilder b) throws IOException {
            b.revision(RevisionImpl.fromString(source.readUTF()));
        }
    }
}
